package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/consensus"
	"github.com/filecoin-project/lotus/chain/stmgr"
)

func main() {
	// Check if we have the required 3 arguments
	if len(os.Args) != 4 {
		log.Fatalf("Usage: %s <actor_cid> <method_number> <params_base64>\n", os.Args[0])
	}

	// Parse command line arguments
	actorCidStr := os.Args[1]
	methodNumStr := os.Args[2]
	paramsBase64 := os.Args[3]

	// Parse actor CID
	actorCode, err := cid.Decode(actorCidStr)
	if err != nil {
		log.Fatalf("Failed to decode actor code CID '%s': %v", actorCidStr, err)
	}

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
