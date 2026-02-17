package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"

	"github.com/filecoin-project/go-state-types/abi"
	actorstypes "github.com/filecoin-project/go-state-types/actors"
	"github.com/filecoin-project/go-state-types/network"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/consensus"
	"github.com/filecoin-project/lotus/chain/stmgr"
)

func main() {
	// Check if we have the required 5 arguments
	if len(os.Args) != 5 {
		log.Fatalf("Usage: %s <actor_address_id> <method_number> <params_base64> <network_version>\n", os.Args[0])
	}

	// Parse command line arguments
	actorName := os.Args[1]
	methodNumStr := os.Args[2]
	paramsBase64 := os.Args[3]
	networkVersionArg := os.Args[4]

	networkVersionF, err := strconv.ParseFloat(networkVersionArg, 64)
	if err != nil {
		log.Fatalf("Failed to parse network version '%s': %v", networkVersionArg, err)
	}
	networkVersion := int64(math.Floor(networkVersionF))
	if networkVersion < 1 {
		log.Fatalf("Failed to parse network version '%s': %v", networkVersionArg, err)
	}

	actorVersion, err := actorstypes.VersionForNetwork(network.Version(networkVersion))
	if err != nil {
		log.Fatalf("Failed to get actor version: %v", err)
	}

	actorCodes, err := actors.GetActorCodeIDs(actorVersion)
	if err != nil {
		log.Fatalf("Failed to get actor code IDs: %v", err)
	}

	actorCode := actorCodes[actorName]

	// Parse method number
	methodNum, err := strconv.Atoi(methodNumStr)
	if err != nil {
		log.Fatalf("Failed to parse method number '%s': %v", methodNumStr, err)
	}

	actorRegistry := consensus.NewActorRegistry()

	// Decode base64 string to bytes
	params, err := base64.StdEncoding.DecodeString(paramsBase64)
	if err != nil {
		log.Fatalf("Failed to decode base64 params: %v", err)
	}

	paramType, err := stmgr.GetParamType(actorRegistry, actorCode, abi.MethodNum(methodNum))
	if err != nil {
		log.Fatalf("Failed to get param type: %v", err)
	}

	// Unmarshal the CBOR-encoded parameters into the correct type
	err = paramType.UnmarshalCBOR(bytes.NewReader(params))
	if err != nil {
		log.Fatalf("Failed to unmarshal CBOR params: %v", err)
	}

	// Convert paramType to JSON string
	jsonBytes, err := json.Marshal(paramType)
	if err != nil {
		log.Fatalf("Failed to marshal paramType to JSON: %v", err)
	}
	fmt.Printf("%s", string(jsonBytes))
}
