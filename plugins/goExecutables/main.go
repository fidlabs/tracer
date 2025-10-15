package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"log"

	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/lotus/chain/consensus"
	"github.com/filecoin-project/lotus/chain/stmgr"
)

func main() {
	//f06, method 4, addVerifier
	verifRegActorCodeStr := "bafk2bzaceak2iqpfy4hw6xyyrf7c4yfh7pl4copzm7t63mokecsxfcnybxnd2"
	verifRegActorCode, err := cid.Decode(verifRegActorCodeStr)
	if err != nil {
		log.Fatalf("Failed to decode actor code CID: %v", err)
	}
	actorRegistry := consensus.NewActorRegistry()

	methods, ok := actorRegistry.Methods[verifRegActorCode]
	if !ok {
		fmt.Printf("No methods found for actor code: %s\n", verifRegActorCode)
		return
	}

	fmt.Printf("Available methods for actor %s:\n", verifRegActorCode)
	for methodNum, meta := range methods {
		fmt.Printf("  Method %d: %s\n", methodNum, meta.Name)
		fmt.Printf("    Params: %s\n", meta.Params)
		fmt.Printf("    Return: %s\n", meta.Ret)
	}

	paramsBase64 := "glUBVSPE42pPyAqB1PrsRUzhEtvbSvdYIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAwAAAAAAAA=="

	// Decode base64 string to bytes
	params, err := base64.StdEncoding.DecodeString(paramsBase64)
	if err != nil {
		log.Fatalf("Failed to decode base64 params: %v", err)
	}

	paramType, err := stmgr.GetParamType(actorRegistry, verifRegActorCode, 9)
	if err != nil {
		log.Fatalf("Failed to get param type: %v", err)
	}

	// Unmarshal the CBOR-encoded parameters into the correct type
	err = paramType.UnmarshalCBOR(bytes.NewReader(params))
	if err != nil {
		log.Fatalf("Failed to unmarshal CBOR params: %v", err)
	}
	fmt.Printf("Decoded parameters: %+v\n", paramType)
}
