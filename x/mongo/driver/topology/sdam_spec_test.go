// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package topology

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/internal/testutil/assert"
	testhelpers "go.mongodb.org/mongo-driver/internal/testutil/helpers"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/address"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
	"go.mongodb.org/mongo-driver/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/x/mongo/driver/uuid"
)

type response struct {
	Host     string
	IsMaster IsMaster
}

type IsMaster struct {
	Arbiters                     []string           `bson:"arbiters,omitempty"`
	ArbiterOnly                  bool               `bson:"arbiterOnly,omitempty"`
	ClusterTime                  bson.Raw           `bson:"$clusterTime,omitempty"`
	Compression                  []string           `bson:"compression,omitempty"`
	ElectionID                   primitive.ObjectID `bson:"electionId,omitempty"`
	Hidden                       bool               `bson:"hidden,omitempty"`
	Hosts                        []string           `bson:"hosts,omitempty"`
	IsMaster                     bool               `bson:"ismaster,omitempty"`
	IsReplicaSet                 bool               `bson:"isreplicaset,omitempty"`
	LastWrite                    *lastWriteDate     `bson:"lastWrite,omitempty"`
	LogicalSessionTimeoutMinutes uint32             `bson:"logicalSessionTimeoutMinutes,omitempty"`
	MaxBSONObjectSize            uint32             `bson:"maxBsonObjectSize,omitempty"`
	MaxMessageSizeBytes          uint32             `bson:"maxMessageSizeBytes,omitempty"`
	MaxWriteBatchSize            uint32             `bson:"maxWriteBatchSize,omitempty"`
	Me                           string             `bson:"me,omitempty"`
	MaxWireVersion               int32              `bson:"maxWireVersion,omitempty"`
	MinWireVersion               int32              `bson:"minWireVersion,omitempty"`
	Msg                          string             `bson:"msg,omitempty"`
	OK                           int32              `bson:"ok"`
	Passives                     []string           `bson:"passives,omitempty"`
	Primary                      string             `bson:"primary,omitempty"`
	ReadOnly                     bool               `bson:"readOnly,omitempty"`
	SaslSupportedMechs           []string           `bson:"saslSupportedMechs,omitempty"`
	Secondary                    bool               `bson:"secondary,omitempty"`
	SetName                      string             `bson:"setName,omitempty"`
	SetVersion                   uint32             `bson:"setVersion,omitempty"`
	Tags                         map[string]string  `bson:"tags,omitempty"`
	TopologyVersion              *topologyVersion   `bson:"topologyVersion,omitempty"`
}

type lastWriteDate struct {
	LastWriteDate time.Time `bson:"lastWriteDate"`
}

type server struct {
	Type            string
	SetName         string
	SetVersion      uint32
	ElectionID      *primitive.ObjectID `bson:"electionId"`
	MinWireVersion  *int32
	MaxWireVersion  *int32
	TopologyVersion *topologyVersion
	Pool            *testPool
}

type topologyVersion struct {
	ProcessID primitive.ObjectID `bson:"processId"`
	Counter   int64
}

type testPool struct {
	Generation uint64
}

type applicationError struct {
	Address        string
	Generation     *uint64
	MaxWireVersion *int32
	When           string
	Type           string
	Response       bsoncore.Document
}

type topologyDescription struct {
	TopologyType string              `json:"topologyType"`
	Servers      []serverDescription `json:"servers"`
	SetName      string              `json:"setName,omitempty"`
}

type serverDescription struct {
	Address  string   `json:"address"`
	Arbiters []string `json:"arbiters"`
	Hosts    []string `json:"hosts"`
	Passives []string `json:"passives"`
	Primary  string   `json:"primary,omitempty"`
	SetName  string   `json:"setName,omitempty"`
	Type     string   `json:"type"`
}

type topologyOpeningEvent struct {
	TopologyID string `json:"topologyId"`
}

type serverOpeningEvent struct {
	Address    string `json:"address"`
	TopologyID string `json:"topologyId"`
}

type topologyDescriptionChangedEvent struct {
	TopologyID          string              `json:"topologyId"`
	PreviousDescription topologyDescription `json:"previousDescription"`
	NewDescription      topologyDescription `json:"newDescription"`
}

type serverDescriptionChangedEvent struct {
	Address             string            `json:"address"`
	TopologyID          string            `json:"topologyId"`
	PreviousDescription serverDescription `json:"previousDescription"`
	NewDescription      serverDescription `json:"newDescription"`
}

type serverClosedEvent struct {
	Address    string `json:"address"`
	TopologyID string `json:"topologyId"`
}

type monitoringEvent struct {
	TopologyOpeningEvent            *topologyOpeningEvent            `json:"topology_opening_event,omitempty"`
	ServerOpeningEvent              *serverOpeningEvent              `json:"server_opening_event,omitempty"`
	TopologyDescriptionChangedEvent *topologyDescriptionChangedEvent `json:"topology_description_changed_event,omitempty"`
	ServerDescriptionChangedEvent   *serverDescriptionChangedEvent   `json:"server_description_changed_event,omitempty"`
	ServerClosedEvent               *serverClosedEvent               `json:"server_closed_event,omitempty"`
}

type outcome struct {
	Servers                      map[string]server
	TopologyType                 string
	SetName                      string
	LogicalSessionTimeoutMinutes uint32
	MaxSetVersion                uint32
	MaxElectionID                primitive.ObjectID `bson:"maxElectionId"`
	Compatible                   *bool
	Events 						 []monitoringEvent
}

type phase struct {
	Description       string
	Responses         []response
	ApplicationErrors []applicationError
	Outcome           outcome
}

type testCase struct {
	Description string
	URI         string
	Phases      []phase
}

func serverDescriptionChanged(e *event.ServerDescriptionChangedEvent) {
	lock.Lock()
	publishedEvents = append(publishedEvents, *e)
	lock.Unlock()
}

func serverOpening(e *event.ServerOpeningEvent) {
	lock.Lock()
	publishedEvents = append(publishedEvents, *e)
	lock.Unlock()
}

func topologyDescriptionChanged(e *event.TopologyDescriptionChangedEvent) {
	lock.Lock()
	publishedEvents = append(publishedEvents, *e)
	lock.Unlock()
}

func topologyOpening(e *event.TopologyOpeningEvent) {
	lock.Lock()
	publishedEvents = append(publishedEvents, *e)
	lock.Unlock()
}

func serverClosed(e *event.ServerClosedEvent) {
	lock.Lock()
	publishedEvents = append(publishedEvents, *e)
	lock.Unlock()
}

const testsDir string = "../../../../data/server-discovery-and-monitoring/"
var publishedEvents []interface{}
var lock sync.Mutex

func (r *response) UnmarshalBSON(buf []byte) error {
	doc := bson.Raw(buf)
	if err := doc.Index(0).Value().Unmarshal(&r.Host); err != nil {
		return fmt.Errorf("error unmarshalling Host: %v", err)
	}

	if err := doc.Index(1).Value().Unmarshal(&r.IsMaster); err != nil {
		return fmt.Errorf("error unmarshalling IsMaster: %v", err)
	}

	return nil
}

func setUpTopology(t *testing.T, uri string) *Topology {
	cs, err := connstring.ParseAndValidate(uri)
	assert.Nil(t, err, "Parse error: %v", err)

	sdam := &event.SdamMonitor{
		ServerDescriptionChanged:   serverDescriptionChanged,
		ServerOpening:              serverOpening,
		TopologyDescriptionChanged: topologyDescriptionChanged,
		TopologyOpening:            topologyOpening,
		ServerClosed:               serverClosed,
	}

	serverOpts := []ServerOption{
		WithServerSdamMonitor(func(*event.SdamMonitor) *event.SdamMonitor { return sdam }),
	}
	topo, err := New(
		WithConnString(func(connstring.ConnString) connstring.ConnString {
			return cs
		}),
		WithSeedList(func(...string) []string { return cs.Hosts }),
		WithServerOptions(func(opts ...ServerOption) []ServerOption {
			return append(opts, serverOpts...)
		}),
		WithTopologySdamMonitor(func(*event.SdamMonitor) *event.SdamMonitor { return sdam }),
	)
	assert.Nil(t, err, "topology.New error: %v", err)

	// add servers to topology without starting heartbeat goroutines
	topo.serversLock.Lock()
	for _, a := range topo.cfg.seedList {
		addr := address.Address(a).Canonicalize()
		topo.fsm.Servers = append(topo.fsm.Servers, description.Server{Addr: addr})

		svr, err := NewServer(addr, topo.cfg.serverOpts...)
		assert.Nil(t, err, "NewServer error: %v", err)
		atomic.StoreInt32(&svr.connectionstate, connected)
		svr.desc.Store(description.NewDefaultServer(svr.address))
		svr.updateTopologyCallback.Store(topo.updateCallback)

		topo.servers[addr] = svr
	}
	topo.desc.Store(description.Topology{Servers: topo.fsm.Servers})
	topo.serversLock.Unlock()

	atomic.StoreInt32(&topo.connectionstate, connected)

	return topo
}

func applyResponses(t *testing.T, topo *Topology, responses []response, sub *driver.Subscription) {
	select {
	case <-sub.Updates:
	default:
	}
	for _, response := range responses {
		doc, err := bson.Marshal(response.IsMaster)
		assert.Nil(t, err, "Marshal error: %v", err)

		addr := address.Address(response.Host)
		desc := description.NewServer(addr, bsoncore.Document(doc))
		server, ok := topo.servers[addr]
		if ok {
			server.updateDescription(desc)
		} else {
			// for tests that check that server descriptions that aren't in the topology aren't applied
			topo.apply(context.Background(), desc)
		}
		select {
		case <-sub.Updates:
		default:
			return
		}
	}
}

type netErr struct {
	timeout bool
}

func (n netErr) Error() string {
	return "error"
}

func (n netErr) Timeout() bool {
	return n.timeout
}

func (n netErr) Temporary() bool {
	return false
}

var _ net.Error = (*netErr)(nil)

// helper method to create an error from the test response
func getError(rdr bsoncore.Document) error {
	var errmsg, codeName string
	var code int32
	var labels []string
	var tv *description.TopologyVersion
	var wcError driver.WriteCommandError
	elems, err := rdr.Elements()
	if err != nil {
		return err
	}

	for _, elem := range elems {
		switch elem.Key() {
		case "ok":
			switch elem.Value().Type {
			case bson.TypeInt32:
				if elem.Value().Int32() == 1 {
					return nil
				}
			case bson.TypeInt64:
				if elem.Value().Int64() == 1 {
					return nil
				}
			case bson.TypeDouble:
				if elem.Value().Double() == 1 {
					return nil
				}
			}
		case "errmsg":
			if str, okay := elem.Value().StringValueOK(); okay {
				errmsg = str
			}
		case "codeName":
			if str, okay := elem.Value().StringValueOK(); okay {
				codeName = str
			}
		case "code":
			if c, okay := elem.Value().Int32OK(); okay {
				code = c
			}
		case "errorLabels":
			if arr, okay := elem.Value().ArrayOK(); okay {
				elems, err := arr.Elements()
				if err != nil {
					continue
				}
				for _, elem := range elems {
					if str, ok := elem.Value().StringValueOK(); ok {
						labels = append(labels, str)
					}
				}

			}
		case "writeErrors":
			arr, exists := elem.Value().ArrayOK()
			if !exists {
				break
			}
			vals, err := arr.Values()
			if err != nil {
				continue
			}
			for _, val := range vals {
				var we driver.WriteError
				doc, exists := val.DocumentOK()
				if !exists {
					continue
				}
				if index, exists := doc.Lookup("index").AsInt64OK(); exists {
					we.Index = index
				}
				if code, exists := doc.Lookup("code").AsInt64OK(); exists {
					we.Code = code
				}
				if msg, exists := doc.Lookup("errmsg").StringValueOK(); exists {
					we.Message = msg
				}
				wcError.WriteErrors = append(wcError.WriteErrors, we)
			}
		case "writeConcernError":
			doc, exists := elem.Value().DocumentOK()
			if !exists {
				break
			}
			wcError.WriteConcernError = new(driver.WriteConcernError)
			if code, exists := doc.Lookup("code").AsInt64OK(); exists {
				wcError.WriteConcernError.Code = code
			}
			if name, exists := doc.Lookup("codeName").StringValueOK(); exists {
				wcError.WriteConcernError.Name = name
			}
			if msg, exists := doc.Lookup("errmsg").StringValueOK(); exists {
				wcError.WriteConcernError.Message = msg
			}
			if info, exists := doc.Lookup("errInfo").DocumentOK(); exists {
				wcError.WriteConcernError.Details = make([]byte, len(info))
				copy(wcError.WriteConcernError.Details, info)
			}
			if errLabels, exists := doc.Lookup("errorLabels").ArrayOK(); exists {
				elems, err := errLabels.Elements()
				if err != nil {
					continue
				}
				for _, elem := range elems {
					if str, ok := elem.Value().StringValueOK(); ok {
						labels = append(labels, str)
					}
				}
			}
		case "topologyVersion":
			doc, ok := elem.Value().DocumentOK()
			if !ok {
				break
			}
			version, err := description.NewTopologyVersion(doc)
			if err == nil {
				tv = version
			}
		}
	}

	if errmsg == "" {
		errmsg = "command failed"
	}

	return driver.Error{
		Code:            code,
		Message:         errmsg,
		Name:            codeName,
		Labels:          labels,
		TopologyVersion: tv,
	}
}

func applyErrors(t *testing.T, topo *Topology, errors []applicationError) {
	for _, appErr := range errors {
		var currError error
		switch appErr.Type {
		case "command":
			currError = getError(appErr.Response)
		case "network":
			currError = driver.Error{
				Labels:  []string{driver.NetworkError},
				Wrapped: ConnectionError{Wrapped: netErr{false}},
			}
		case "timeout":
			currError = driver.Error{
				Labels:  []string{driver.NetworkError},
				Wrapped: ConnectionError{Wrapped: netErr{true}},
			}
		default:
			t.Fatalf("unrecognized error type: %v", appErr.Type)
		}
		server, ok := topo.servers[address.Address(appErr.Address)]
		assert.True(t, ok, "server not found: %v", appErr.Address)

		desc := server.Description()
		versionRange := description.NewVersionRange(0, *appErr.MaxWireVersion)
		desc.WireVersion = &versionRange

		generation := atomic.LoadUint64(&server.pool.generation)
		if appErr.Generation != nil {
			generation = uint64(*appErr.Generation)
		}
		//use generation number to check conn stale
		innerConn := connection{
			desc:       desc,
			generation: generation,
			pool:       server.pool,
		}
		conn := Connection{connection: &innerConn}

		switch appErr.When {
		case "beforeHandshakeCompletes":
			server.ProcessHandshakeError(currError, generation)
		case "afterHandshakeCompletes":
			server.ProcessError(currError, &conn)
		default:
			t.Fatalf("unrecognized applicationError.When value: %v", appErr.When)
		}
	}
}

func compareServerDescriptions(t *testing.T,
	e serverDescription, a event.ServerDescription) {
	assert.Equal(t, e.Address, string(a.Address),
		"expected server address %s, got %s", e.Address, a.Address)

	assert.Equal(t, len(e.Hosts), len(a.Hosts),
		"expected %d hosts, got %d", len(e.Hosts), len(a.Hosts))
	for idx, eh := range e.Hosts {
		ah := a.Hosts[idx]
		assert.Equal(t, eh, string(ah), "expected host %s, got %s", eh, ah)
	}

	assert.Equal(t, len(e.Passives), len(a.Passives),
		"expected %d hosts, got %d", len(e.Passives), len(a.Passives))
	for idx, ep := range e.Passives {
		ap := a.Passives[idx]
		assert.Equal(t, ep, string(ap), "expected passive %s, got %s", ep, ap)
	}

	assert.Equal(t, e.Primary, string(a.Primary),
		"expected primary %s, got %s", e.Primary, a.Primary)
	assert.Equal(t, e.SetName, a.SetName,
		"expected set name %s, got %s", e.SetName, a.SetName)

	// PossiblePrimary is only relevant to single-threaded drivers.
	if e.Type == "PossiblePrimary" {
		e.Type = "Unknown"
	}
	assert.Equal(t, e.Type, a.Kind.String(),
		"expected server kind %s, got %s", e.Type, a.Kind.String())
}

func compareTopologyDescriptions(t *testing.T,
	e topologyDescription, a event.TopologyDescription) {
	assert.Equal(t, e.TopologyType, a.Kind.String(),
		"expected topology kind %s, got %s", e.TopologyType, a.Kind.String())
	assert.Equal(t, len(e.Servers), len(a.Servers),
		"expected %d servers, got %d", len(e.Servers), len(a.Servers))

	for idx, es := range e.Servers {
		as := a.Servers[idx]
		compareServerDescriptions(t, es, as)
	}

	assert.Equal(t, e.SetName, a.SetName,
		"expected set name %s, got %s", e.SetName, a.SetName)
}

func compareEvents(t *testing.T, events []monitoringEvent) {
	var emptyUUID uuid.UUID

	for idx, me := range events {

		if me.TopologyOpeningEvent != nil {
			actual, ok := publishedEvents[idx].(event.TopologyOpeningEvent)
			assert.True(t, ok, "expected type %T, got %T", event.TopologyOpeningEvent{}, actual)
			assert.False(t, uuid.Equal(uuid.UUID(actual.ID), emptyUUID), "expected topology id")
		}
		if me.ServerOpeningEvent != nil {
			actual, ok := publishedEvents[idx].(event.ServerOpeningEvent)
			assert.True(t, ok, "expected type %T, got %T", event.ServerOpeningEvent{}, actual)

			evt := me.ServerOpeningEvent
			assert.Equal(t, evt.Address, string(actual.Address),
				"expected address %s, got %s", evt.Address, actual.Address)
			// TODO: assert.False(t, uuid.Equal(uuid.UUID(actual.ID), emptyUUID), "expected topology id %v", actual.ID)
		}
		if me.TopologyDescriptionChangedEvent != nil {
			actual, ok := publishedEvents[idx].(event.TopologyDescriptionChangedEvent)
			assert.True(t, ok, "expected type %T, got %T", event.TopologyDescriptionChangedEvent{}, actual)

			evt := me.TopologyDescriptionChangedEvent
			compareTopologyDescriptions(t, evt.PreviousDescription, actual.PreviousDescription)
			compareTopologyDescriptions(t, evt.NewDescription, actual.NewDescription)
			assert.False(t, uuid.Equal(uuid.UUID(actual.ID), emptyUUID), "expected topology id")
		}
		if me.ServerDescriptionChangedEvent != nil {
			actual, ok := publishedEvents[idx].(event.ServerDescriptionChangedEvent)
			assert.True(t, ok, "expected type %T, got %T", event.ServerDescriptionChangedEvent{}, actual)

			evt := me.ServerDescriptionChangedEvent
			assert.Equal(t, evt.Address, string(actual.Address),
				"expected server address %s, got %s", evt.Address, actual.Address)
			compareServerDescriptions(t, evt.PreviousDescription, actual.PreviousDescription)
			compareServerDescriptions(t, evt.NewDescription, actual.NewDescription)
			assert.False(t, uuid.Equal(uuid.UUID(actual.ID), emptyUUID), "expected topology id")
		}
		if me.ServerClosedEvent != nil {
			actual, ok := publishedEvents[idx].(event.ServerClosedEvent)
			assert.True(t, ok, "expected type %T, got %T", event.ServerClosedEvent{}, actual)

			evt := me.ServerClosedEvent
			assert.Equal(t, evt.Address, string(actual.Address),
				"expected server address %s, got %s", evt.Address, actual.Address)
			assert.False(t, uuid.Equal(uuid.UUID(actual.ID), emptyUUID), "expected topology id")
		}
	}

}

func runTest(t *testing.T, directory string, filename string) {
	filepath := path.Join(testsDir, directory, filename)
	content, err := ioutil.ReadFile(filepath)
	assert.Nil(t, err, "ReadFile error: %v", err)

	// Remove ".json" from filename.
	filename = filename[:len(filename)-5]
	testName := directory + "/" + filename + ":"

	t.Run(testName, func(t *testing.T) {
		var test testCase
		err = bson.UnmarshalExtJSON(content, false, &test)
		assert.Nil(t, err, "Unmarshal error: %v", err)
		topo := setUpTopology(t, test.URI)
		sub, err := topo.Subscribe()
		assert.Nil(t, err, "subscribe error: %v", err)

		for _, phase := range test.Phases {
			applyResponses(t, topo, phase.Responses, sub)
			applyErrors(t, topo, phase.ApplicationErrors)
			if phase.Outcome.Compatible == nil || *phase.Outcome.Compatible {
				assert.True(t, topo.fsm.compatible.Load().(bool), "Expected servers to be compatible")
				assert.Nil(t, topo.fsm.compatibilityErr, "expected fsm.compatiblity to be nil, got %v",
					topo.fsm.compatibilityErr)
			} else {
				assert.False(t, topo.fsm.compatible.Load().(bool), "Expected servers to not be compatible")
				assert.NotNil(t, topo.fsm.compatibilityErr, "expected fsm.compatiblity error to be non-nil")
				continue
			}
			desc := topo.Description()

			assert.Equal(t, phase.Outcome.TopologyType, desc.Kind.String(),
				"expected TopologyType to be %v, got %v", phase.Outcome.TopologyType, desc.Kind.String())
			assert.Equal(t, phase.Outcome.SetName, topo.fsm.SetName,
				"expected SetName to be %v, got %v", phase.Outcome.SetName, topo.fsm.SetName)
			assert.Equal(t, len(phase.Outcome.Servers), len(desc.Servers),
				"expected %v servers, got %v", len(phase.Outcome.Servers), len(desc.Servers))
			assert.Equal(t, phase.Outcome.LogicalSessionTimeoutMinutes, desc.SessionTimeoutMinutes,
				"expected SessionTimeoutMinutes to be %v, got %v", phase.Outcome.LogicalSessionTimeoutMinutes, desc.SessionTimeoutMinutes)
			assert.Equal(t, phase.Outcome.MaxSetVersion, topo.fsm.maxSetVersion,
				"expected maxSetVersion to be %v, got %v", phase.Outcome.MaxSetVersion, topo.fsm.maxSetVersion)
			assert.Equal(t, phase.Outcome.MaxElectionID, topo.fsm.maxElectionID,
				"expected maxElectionID to be %v, got %v", phase.Outcome.MaxElectionID, topo.fsm.maxElectionID)

			for addr, server := range phase.Outcome.Servers {
				fsmServer, ok := desc.Server(address.Address(addr))
				assert.True(t, ok, "Couldn't find server %v", addr)

				assert.Equal(t, address.Address(addr), fsmServer.Addr,
					"expected server address to be %v, got %v", address.Address(addr), fsmServer.Addr)
				assert.Equal(t, server.SetName, fsmServer.SetName,
					"expected server SetName to be %v, got %v", server.SetName, fsmServer.SetName)
				assert.Equal(t, server.SetVersion, fsmServer.SetVersion,
					"expected server SetVersion to be %v, got %v", server.SetVersion, fsmServer.SetVersion)
				if server.ElectionID != nil {
					assert.Equal(t, *server.ElectionID, fsmServer.ElectionID,
						"expected server ElectionID to be %v, got %v", *server.ElectionID, fsmServer.ElectionID)
				}
				if server.TopologyVersion != nil {

					assert.NotNil(t, fsmServer.TopologyVersion, "expected server TopologyVersion not to be nil")
					assert.Equal(t, server.TopologyVersion.ProcessID, fsmServer.TopologyVersion.ProcessID,
						"expected server TopologyVersion ProcessID to be %v, got %v", server.TopologyVersion.ProcessID, fsmServer.TopologyVersion.ProcessID)
					assert.Equal(t, server.TopologyVersion.Counter, fsmServer.TopologyVersion.Counter,
						"expected server TopologyVersion Counter to be %v, got %v", server.TopologyVersion.Counter, fsmServer.TopologyVersion.Counter)
				} else {
					assert.Nil(t, fsmServer.TopologyVersion, "expected server TopologyVersion to be nil")
				}

				// PossiblePrimary is only relevant to single-threaded drivers.
				if server.Type == "PossiblePrimary" {
					server.Type = "Unknown"
				}

				assert.Equal(t, server.Type, fsmServer.Kind.String(),
					"expected server Type to be %v, got %v", server.Type, fsmServer.Kind.String())
				if server.Pool != nil {
					topo.serversLock.Lock()
					actualServer := topo.servers[address.Address(addr)]
					topo.serversLock.Unlock()
					actualGeneration := atomic.LoadUint64(&actualServer.pool.generation)
					assert.Equal(t, server.Pool.Generation, actualGeneration,
						"expected server pool generation to be %v, got %v", server.Pool.Generation, actualGeneration)
				}
			}

			lock.Lock()
			defer lock.Unlock()

			assert.Equal(t, len(phase.Outcome.Events), len(publishedEvents),
				"expected %d published events, got %d\n",
				len(phase.Outcome.Events), len(publishedEvents))

			compareEvents(t, phase.Outcome.Events)
		}

		publishedEvents = nil
	})
}

// Test case for all SDAM spec tests.
func TestSDAMSpec(t *testing.T) {
	//for _, subdir := range []string{"single", "rs", "sharded", "errors"} {
	for _, subdir := range []string{"monitoring"} {
		for _, file := range testhelpers.FindJSONFilesInDir(t, path.Join(testsDir, subdir)) {
			runTest(t, subdir, file)
		}
	}
}
