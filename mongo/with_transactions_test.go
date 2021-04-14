// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongo

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/internal/testutil"
	"go.mongodb.org/mongo-driver/internal/testutil/assert"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
)

var (
	connsCheckedOut        int
	errorInterrupted       int32 = 11601
	withTxnStartedEvents   []*event.CommandStartedEvent
	withTxnSucceededEvents []*event.CommandSucceededEvent
	withTxnFailedEvents    []*event.CommandFailedEvent
)

type wrappedError struct {
	err error
}

func (we wrappedError) Error() string {
	return we.err.Error()
}

func (we wrappedError) Unwrap() error {
	return we.err
}

func TestConvenientTransactions(t *testing.T) {
	client := setupConvenientTransactions(t)
	db := client.Database("TestConvenientTransactions")
	dbAdmin := client.Database("admin")

	defer func() {
		sessions := client.NumberSessionsInProgress()
		conns := connsCheckedOut

		err := dbAdmin.RunCommand(bgCtx, bson.D{
			{"killAllSessions", bson.A{}},
		}).Err()
		if err != nil {
			if ce, ok := err.(CommandError); !ok || ce.Code != errorInterrupted {
				t.Fatalf("killAllSessions error: %v", err)
			}
		}

		_ = db.Drop(bgCtx)
		_ = client.Disconnect(bgCtx)

		assert.Equal(t, 0, sessions, "%v sessions checked out", sessions)
		assert.Equal(t, 0, conns, "%v connections checked out", conns)
	}()

	t.Run("callback raises custom error", func(t *testing.T) {
		coll := db.Collection(t.Name())
		_, err := coll.InsertOne(bgCtx, bson.D{{"x", 1}})
		assert.Nil(t, err, "InsertOne error: %v", err)

		sess, err := client.StartSession()
		assert.Nil(t, err, "StartSession error: %v", err)
		defer sess.EndSession(context.Background())

		testErr := errors.New("test error")
		_, err = sess.WithTransaction(context.Background(), func(sessCtx SessionContext) (interface{}, error) {
			return nil, testErr
		})
		assert.Equal(t, testErr, err, "expected error %v, got %v", testErr, err)
	})
	t.Run("callback returns value", func(t *testing.T) {
		coll := db.Collection(t.Name())
		_, err := coll.InsertOne(bgCtx, bson.D{{"x", 1}})
		assert.Nil(t, err, "InsertOne error: %v", err)

		sess, err := client.StartSession()
		assert.Nil(t, err, "StartSession error: %v", err)
		defer sess.EndSession(context.Background())

		res, err := sess.WithTransaction(context.Background(), func(sessCtx SessionContext) (interface{}, error) {
			return false, nil
		})
		assert.Nil(t, err, "WithTransaction error: %v", err)
		resBool, ok := res.(bool)
		assert.True(t, ok, "expected result type %T, got %T", false, res)
		assert.False(t, resBool, "expected result false, got %v", resBool)
	})
	t.Run("retry timeout enforced", func(t *testing.T) {
		withTransactionTimeout = time.Second

		coll := db.Collection(t.Name())
		_, err := coll.InsertOne(bgCtx, bson.D{{"x", 1}})
		assert.Nil(t, err, "InsertOne error: %v", err)

		t.Run("transient transaction error", func(t *testing.T) {
			sess, err := client.StartSession()
			assert.Nil(t, err, "StartSession error: %v", err)
			defer sess.EndSession(context.Background())

			_, err = sess.WithTransaction(context.Background(), func(sessCtx SessionContext) (interface{}, error) {
				return nil, CommandError{Name: "test Error", Labels: []string{driver.TransientTransactionError}}
			})
			assert.NotNil(t, err, "expected WithTransaction error, got nil")
			cmdErr, ok := err.(CommandError)
			assert.True(t, ok, "expected error type %T, got %T", CommandError{}, err)
			assert.True(t, cmdErr.HasErrorLabel(driver.TransientTransactionError),
				"expected error with label %v, got %v", driver.TransientTransactionError, cmdErr)
		})
		t.Run("unknown transaction commit result", func(t *testing.T) {
			//set failpoint
			failpoint := bson.D{{"configureFailPoint", "failCommand"},
				{"mode", "alwaysOn"},
				{"data", bson.D{
					{"failCommands", bson.A{"commitTransaction"}},
					{"closeConnection", true},
				}},
			}
			err = dbAdmin.RunCommand(bgCtx, failpoint).Err()
			assert.Nil(t, err, "error setting failpoint: %v", err)
			defer func() {
				err = dbAdmin.RunCommand(bgCtx, bson.D{
					{"configureFailPoint", "failCommand"},
					{"mode", "off"},
				}).Err()
				assert.Nil(t, err, "error turning off failpoint: %v", err)
			}()

			sess, err := client.StartSession()
			assert.Nil(t, err, "StartSession error: %v", err)
			defer sess.EndSession(context.Background())

			_, err = sess.WithTransaction(context.Background(), func(sessCtx SessionContext) (interface{}, error) {
				_, err := coll.InsertOne(sessCtx, bson.D{{"x", 1}})
				return nil, err
			})
			assert.NotNil(t, err, "expected WithTransaction error, got nil")
			cmdErr, ok := err.(CommandError)
			assert.True(t, ok, "expected error type %T, got %T", CommandError{}, err)
			assert.True(t, cmdErr.HasErrorLabel(driver.UnknownTransactionCommitResult),
				"expected error with label %v, got %v", driver.UnknownTransactionCommitResult, cmdErr)
		})
		t.Run("commit transient transaction error", func(t *testing.T) {
			//set failpoint
			failpoint := bson.D{{"configureFailPoint", "failCommand"},
				{"mode", "alwaysOn"},
				{"data", bson.D{
					{"failCommands", bson.A{"commitTransaction"}},
					{"errorCode", 251},
				}},
			}
			err = dbAdmin.RunCommand(bgCtx, failpoint).Err()
			assert.Nil(t, err, "error setting failpoint: %v", err)
			defer func() {
				err = dbAdmin.RunCommand(bgCtx, bson.D{
					{"configureFailPoint", "failCommand"},
					{"mode", "off"},
				}).Err()
				assert.Nil(t, err, "error turning off failpoint: %v", err)
			}()

			sess, err := client.StartSession()
			assert.Nil(t, err, "StartSession error: %v", err)
			defer sess.EndSession(context.Background())

			_, err = sess.WithTransaction(context.Background(), func(sessCtx SessionContext) (interface{}, error) {
				_, err := coll.InsertOne(sessCtx, bson.D{{"x", 1}})
				return nil, err
			})
			assert.NotNil(t, err, "expected WithTransaction error, got nil")
			cmdErr, ok := err.(CommandError)
			assert.True(t, ok, "expected error type %T, got %T", CommandError{}, err)
			assert.True(t, cmdErr.HasErrorLabel(driver.TransientTransactionError),
				"expected error with label %v, got %v", driver.TransientTransactionError, cmdErr)
		})
	})
	t.Run("abortTransaction does not time out", func(t *testing.T) {
		// Create a special CommandMonitor that only records information about abortTransaction events and also
		// records the Context used in the CommandStartedEvent listener.
		var abortStarted []*event.CommandStartedEvent
		var abortSucceeded []*event.CommandSucceededEvent
		var abortFailed []*event.CommandFailedEvent
		var abortCtx context.Context
		monitor := &event.CommandMonitor{
			Started: func(ctx context.Context, evt *event.CommandStartedEvent) {
				if evt.CommandName == "abortTransaction" {
					abortStarted = append(abortStarted, evt)
					if abortCtx == nil {
						abortCtx = ctx
					}
				}
			},
			Succeeded: func(_ context.Context, evt *event.CommandSucceededEvent) {
				if evt.CommandName == "abortTransaction" {
					abortSucceeded = append(abortSucceeded, evt)
				}
			},
			Failed: func(_ context.Context, evt *event.CommandFailedEvent) {
				if evt.CommandName == "abortTransaction" {
					abortFailed = append(abortFailed, evt)
				}
			},
		}

		// Set up a new Client using the command monitor defined above get a handle to a collection. The collection
		// needs to be explicitly created on the server because implicit collection creation is not allowed in
		// transactions for server versions <= 4.2.
		client := setupConvenientTransactions(t, options.Client().SetMonitor(monitor))
		db := client.Database("foo")
		coll := db.Collection("bar")
		err := db.RunCommand(bgCtx, bson.D{{"create", coll.Name()}}).Err()
		assert.Nil(t, err, "error creating collection on server: %v\n", err)

		sess, err := client.StartSession()
		assert.Nil(t, err, "StartSession error: %v", err)
		defer func() {
			sess.EndSession(bgCtx)
			_ = coll.Drop(bgCtx)
			_ = client.Disconnect(bgCtx)
		}()

		// Create a cancellable Context with a value for ctxKey.
		type ctxKey struct{}
		ctx, cancel := context.WithCancel(context.WithValue(context.Background(), ctxKey{}, "foobar"))
		defer cancel()

		// The WithTransaction callback does an Insert to ensure that the txn has been started server-side. After the
		// insert succeeds, it cancels the Context created above and returns a non-retryable error, which forces
		// WithTransaction to abort the txn.
		callbackErr := errors.New("error")
		callback := func(sc SessionContext) (interface{}, error) {
			_, err = coll.InsertOne(sc, bson.D{{"x", 1}})
			if err != nil {
				return nil, err
			}

			cancel()
			return nil, callbackErr
		}

		_, err = sess.WithTransaction(ctx, callback)
		assert.Equal(t, callbackErr, err, "expected WithTransaction error %v, got %v", callbackErr, err)

		// Assert that abortTransaction was sent once and succeede.
		assert.Equal(t, 1, len(abortStarted), "expected 1 abortTransaction started event, got %d", len(abortStarted))
		assert.Equal(t, 1, len(abortSucceeded), "expected 1 abortTransaction succeeded event, got %d",
			len(abortSucceeded))
		assert.Equal(t, 0, len(abortFailed), "expected 0 abortTransaction failed event, got %d", len(abortFailed))

		// Assert that the Context propagated to the CommandStartedEvent listener for abortTransaction contained a value
		// for ctxKey.
		ctxValue, ok := abortCtx.Value(ctxKey{}).(string)
		assert.True(t, ok, "expected context for abortTransaction to contain ctxKey")
		assert.Equal(t, "foobar", ctxValue, "expected value for ctxKey to be 'world', got %s", ctxValue)
	})
	t.Run("commitTransaction timeout allows abortTransaction", func(t *testing.T) {
		// Create a special CommandMonitor that only records information about abortTransaction events.
		var abortStarted []*event.CommandStartedEvent
		var abortSucceeded []*event.CommandSucceededEvent
		var abortFailed []*event.CommandFailedEvent
		monitor := &event.CommandMonitor{
			Started: func(ctx context.Context, evt *event.CommandStartedEvent) {
				if evt.CommandName == "abortTransaction" {
					abortStarted = append(abortStarted, evt)
				}
			},
			Succeeded: func(_ context.Context, evt *event.CommandSucceededEvent) {
				if evt.CommandName == "abortTransaction" {
					abortSucceeded = append(abortSucceeded, evt)
				}
			},
			Failed: func(_ context.Context, evt *event.CommandFailedEvent) {
				if evt.CommandName == "abortTransaction" {
					abortFailed = append(abortFailed, evt)
				}
			},
		}

		// Set up a new Client using the command monitor defined above get a handle to a collection. The collection
		// needs to be explicitly created on the server because implicit collection creation is not allowed in
		// transactions for server versions <= 4.2.
		client := setupConvenientTransactions(t, options.Client().SetMonitor(monitor))
		db := client.Database("foo")
		coll := db.Collection("test")
		defer func() {
			_ = coll.Drop(bgCtx)
		}()

		err := db.RunCommand(bgCtx, bson.D{{"create", coll.Name()}}).Err()
		assert.Nil(t, err, "error creating collection on server: %v", err)

		// Start session.
		session, err := client.StartSession()
		defer session.EndSession(bgCtx)
		assert.Nil(t, err, "StartSession error: %v", err)

		_ = WithSession(bgCtx, session, func(sessionContext SessionContext) error {
			// Start transaction.
			err = session.StartTransaction()
			assert.Nil(t, err, "StartTransaction error: %v", err)

			// Insert a document.
			_, err := coll.InsertOne(sessionContext, bson.D{{"val", 17}})
			assert.Nil(t, err, "InsertOne error: %v", err)

			// Set a timeout of 0 for commitTransaction.
			commitTimeoutCtx, commitCancel := context.WithTimeout(sessionContext, 0)
			defer commitCancel()

			// CommitTransaction results in context.DeadlineExceeded.
			commitErr := session.CommitTransaction(commitTimeoutCtx)
			assert.True(t, IsTimeout(commitErr),
				"expected timeout error error; got %v", commitErr)

			// Assert session state is not Committed.
			clientSession := session.(XSession).ClientSession()
			assert.False(t, clientSession.TransactionCommitted(), "expected session state to not be Committed")

			// AbortTransaction without error.
			abortErr := session.AbortTransaction(context.Background())
			assert.Nil(t, abortErr, "AbortTransaction error: %v", abortErr)

			// Assert that AbortTransaction was started once and succeeded.
			assert.Equal(t, 1, len(abortStarted), "expected 1 abortTransaction started event, got %d", len(abortStarted))
			assert.Equal(t, 1, len(abortSucceeded), "expected 1 abortTransaction succeeded event, got %d",
				len(abortSucceeded))
			assert.Equal(t, 0, len(abortFailed), "expected 0 abortTransaction failed events, got %d", len(abortFailed))

			return nil
		})
	})
	t.Run("commitTransaction timeout does not retry", func(t *testing.T) {
		withTransactionTimeout = 2 * time.Second

		coll := db.Collection("test")
		// Explicitly create the collection on server because implicit collection creation is not allowed in
		// transactions for server versions <= 4.2.
		err := db.RunCommand(bgCtx, bson.D{{"create", coll.Name()}}).Err()
		assert.Nil(t, err, "error creating collection on server: %v", err)
		defer func() {
			_ = coll.Drop(bgCtx)
		}()

		// Start session.
		sess, err := client.StartSession()
		assert.Nil(t, err, "StartSession error: %v", err)
		defer sess.EndSession(context.Background())

		// Defer running killAllSessions to manually close open transaction.
		defer func() {
			err := dbAdmin.RunCommand(bgCtx, bson.D{
				{"killAllSessions", bson.A{}},
			}).Err()
			if err != nil {
				if ce, ok := err.(CommandError); !ok || ce.Code != errorInterrupted {
					t.Fatalf("killAllSessions error: %v", err)
				}
			}
		}()

		// Create context to manually cancel in callback.
		cancelCtx, cancel := context.WithCancel(bgCtx)
		defer cancel()

		// Insert a document within a session and manually cancel context.
		callback := func() {
			_, _ = sess.WithTransaction(cancelCtx, func(sessCtx SessionContext) (interface{}, error) {
				_, err := coll.InsertOne(sessCtx, bson.M{"x": 1})
				assert.Nil(t, err, "InsertOne error: %v", err)
				cancel()
				return nil, nil
			})
		}

		// Assert that transaction is canceled within 500ms and not 2 seconds.
		assert.Soon(t, callback, 500*time.Millisecond)
	})
	t.Run("wrapped transient transaction error retried", func(t *testing.T) {
		sess, err := client.StartSession()
		assert.Nil(t, err, "StartSession error: %v", err)
		defer sess.EndSession(context.Background())

		returnError := true
		res, err := sess.WithTransaction(context.Background(), func(sessCtx SessionContext) (interface{}, error) {
			if returnError {
				returnError = false
				return nil, wrappedError{CommandError{Name: "test Error", Labels: []string{driver.TransientTransactionError}}}
			}
			return false, nil
		})
		assert.Nil(t, err, "WithTransaction error: %v", err)
		resBool, ok := res.(bool)
		assert.True(t, ok, "expected result type %T, got %T", false, res)
		assert.False(t, resBool, "expected result false, got %v", resBool)
	})
}

func setupConvenientTransactions(t *testing.T, extraClientOpts ...*options.ClientOptions) *Client {
	cs := testutil.ConnString(t)
	poolMonitor := &event.PoolMonitor{
		Event: func(evt *event.PoolEvent) {
			switch evt.Type {
			case event.GetSucceeded:
				connsCheckedOut++
			case event.ConnectionReturned:
				connsCheckedOut--
			}
		},
	}

	baseClientOpts := options.Client().
		ApplyURI(cs.Original).
		SetReadPreference(readpref.Primary()).
		SetWriteConcern(writeconcern.New(writeconcern.WMajority())).
		SetPoolMonitor(poolMonitor)
	testutil.AddTestServerAPIVersion(baseClientOpts)
	fullClientOpts := []*options.ClientOptions{baseClientOpts}
	fullClientOpts = append(fullClientOpts, extraClientOpts...)

	client, err := Connect(bgCtx, fullClientOpts...)
	assert.Nil(t, err, "Connect error: %v", err)

	version, err := getServerVersion(client.Database("admin"))
	assert.Nil(t, err, "getServerVersion error: %v", err)
	topoKind := client.deployment.(*topology.Topology).Kind()
	if compareVersions(t, version, "4.1") < 0 || topoKind == description.Single {
		t.Skip("skipping standalones and versions < 4.1")
	}

	if topoKind != description.Sharded {
		return client
	}

	// For sharded clusters, disconnect the previous Client and create a new one that's pinned to a single mongos.
	_ = client.Disconnect(bgCtx)
	fullClientOpts = append(fullClientOpts, options.Client().SetHosts([]string{cs.Hosts[0]}))
	client, err = Connect(bgCtx, fullClientOpts...)
	assert.Nil(t, err, "Connect error: %v", err)
	return client
}

func getServerVersion(db *Database) (string, error) {
	serverStatus, err := db.RunCommand(
		context.Background(),
		bson.D{{"serverStatus", 1}},
	).DecodeBytes()
	if err != nil {
		return "", err
	}

	version, err := serverStatus.LookupErr("version")
	if err != nil {
		return "", err
	}

	return version.StringValue(), nil
}

// compareVersions compares two version number strings (i.e. positive integers separated by
// periods). Comparisons are done to the lesser precision of the two versions. For example, 3.2 is
// considered equal to 3.2.11, whereas 3.2.0 is considered less than 3.2.11.
//
// Returns a positive int if version1 is greater than version2, a negative int if version1 is less
// than version2, and 0 if version1 is equal to version2.
func compareVersions(t *testing.T, v1 string, v2 string) int {
	n1 := strings.Split(v1, ".")
	n2 := strings.Split(v2, ".")

	for i := 0; i < int(math.Min(float64(len(n1)), float64(len(n2)))); i++ {
		i1, err := strconv.Atoi(n1[i])
		if err != nil {
			return 1
		}

		i2, err := strconv.Atoi(n2[i])
		if err != nil {
			return -1
		}

		difference := i1 - i2
		if difference != 0 {
			return difference
		}
	}

	return 0
}
