package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/donomii/ensemblekv" // Replace with your actual import path
)

func main() {
	// Command line flags
	storeType := flag.String("type", "", "Store type (ensemble, tree, star, line)")
	baseType := flag.String("base", "extent", "Base store type for ensemble/tree/star (extent, bolt, json,pudge)")
	storeDir := flag.String("dir", "", "Directory containing the store")
	key_h := flag.String("keyhex", "", "Print history for this key")
	key_s := flag.String("key", "", "Print history for this key")
	outputDir := flag.String("output", "", "Output directory for recovered values")
	valueToSet := flag.String("value", "", "Set this value for the key")
	valueFile := flag.String("value-file", "", "Set value from this file for the key")
	flag.Parse()

	if *storeDir == "" {
		log.Fatal("Please specify store directory with -dir")
	}

	if *storeType == "" {
		log.Fatal("Please specify store type with -type")
	}

	// Open the store
	store, err := openStore(*storeType, *baseType, *storeDir)
	if err != nil {
		log.Fatalf("Failed to open store: %v", err)
	}
	defer store.Close()

	var key_b []byte
	key_b = []byte(*key_s)
	if *key_h != "" {
		// Convert hex key to bytes
		keyb, err := hex.DecodeString(*key_h)
		if err != nil {
			log.Fatalf("Invalid hex key: %v", err)
		}
		key_b = keyb
	}

	// Check if we're setting a value
	if *valueToSet != "" || *valueFile != "" {
		if len(key_b) == 0 {
			log.Fatal("Please specify a key with --key or --keyhex when setting a value")
		}

		var valueData []byte
		if *valueFile != "" {
			// Read value from file
			valueData, err = os.ReadFile(*valueFile)
			if err != nil {
				log.Fatalf("Failed to read value file: %v", err)
			}
			fmt.Printf("Read %d bytes from file %s\n", len(valueData), *valueFile)
		} else {
			// Use direct value
			valueData = []byte(*valueToSet)
		}

		// Set the value
		err = store.Put(key_b, valueData)
		if err != nil {
			log.Fatalf("Failed to set value: %v", err)
		}
		fmt.Printf("Successfully set value for key %s (%d bytes)\n",
			hex.EncodeToString(key_b), len(valueData))
		return
	}

	// Get history for the key
	values, err := store.KeyHistory(key_b)
	if err != nil {
		log.Printf("Warning: %v", err)
	}

	if len(values) == 0 {
		fmt.Println("No values found for key:", *key_h)
		return
	}

	fmt.Printf("Found %d values for key %s\n", len(values), *key_h)

	// Output each value summary
	for i, value := range values {
		fmt.Printf("\n--- Value #%d ---\n", i+1)
		if len(value) > 40 {
			fmt.Printf("First 40 bytes: %s\n", hex.EncodeToString(value[:40]))
			fmt.Printf("Length: %d bytes\n", len(value))
		} else {
			fmt.Printf("Full value: %s\n", hex.EncodeToString(value))
		}

		// Try to print as text if it looks printable
		if isPrintableASCII(value) {
			if len(value) > 100 {
				fmt.Printf("As text (first 100 chars): %s...\n", string(value[:100]))
			} else {
				fmt.Printf("As text: %s\n", string(value))
			}
		}
	}

	// Save to files if requested
	if *outputDir != "" {
		saveValuesToFiles(values, *outputDir, *key_h)
	}
}

// openStore creates the appropriate store based on command-line arguments
func openStore(storeType, baseType, storeDir string) (ensemblekv.KvLike, error) {
	var baseCreator ensemblekv.CreatorFunc

	// Determine base store creator
	switch strings.ToLower(baseType) {
	case "extent":
		baseCreator = ensemblekv.ExtentCreator

	case "bolt":
		baseCreator = ensemblekv.BoltDbCreator
	case "json":
		baseCreator = ensemblekv.JsonKVCreator
	case "sqlite":
		baseCreator = ensemblekv.SQLiteCreator
	default:
		return nil, fmt.Errorf("unknown base store type: %s", baseType)
	}

	// Create the appropriate store type
	blockSize := int64(4096) // Default block size
	fileSize := int64(1024 * 1024 * 1024)
	switch strings.ToLower(storeType) {
	case "extent":
		return ensemblekv.ExtentCreator(storeDir, blockSize, fileSize)

	case "bolt":
		return ensemblekv.BoltDbCreator(storeDir, blockSize, fileSize)
	case "json":
		return ensemblekv.JsonKVCreator(storeDir, blockSize, fileSize)
	case "sqlite":
		return ensemblekv.SQLiteCreator(storeDir, blockSize, fileSize)
	case "ensemble":
		return ensemblekv.EnsembleCreator(storeDir, blockSize, fileSize, baseCreator)
	case "tree":
		return ensemblekv.NewTreeLSM(storeDir, blockSize, fileSize, 0, baseCreator)
	case "star":
		return ensemblekv.NewStarLSM(storeDir, blockSize, fileSize, baseCreator)
	case "line":
		return ensemblekv.LineLSMCreator(storeDir, blockSize, fileSize, baseCreator)
	default:
		return nil, fmt.Errorf("unknown store type: %s", storeType)
	}
}

// isPrintableASCII checks if a byte slice is mostly printable ASCII
func isPrintableASCII(data []byte) bool {
	if len(data) == 0 {
		return false
	}

	printable := 0
	for _, b := range data {
		if (b >= 32 && b <= 126) || b == '\n' || b == '\r' || b == '\t' {
			printable++
		}
	}

	// At least 80% should be printable for it to be considered text
	return float64(printable)/float64(len(data)) > 0.8
}

// saveValuesToFiles saves each value to a separate file
func saveValuesToFiles(values [][]byte, outputDir, keyHex string) {
	// Create output directory if needed
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Save each value to a separate file
	for i, value := range values {
		fileName := filepath.Join(outputDir, fmt.Sprintf("%svalue%d.bin", keyHex, i+1))
		if err := os.WriteFile(fileName, value, 0644); err != nil {
			log.Printf("Failed to write value %d to file: %v", i+1, err)
			continue
		}

		// If value looks like text, also save a text version
		if isPrintableASCII(value) {
			textFileName := filepath.Join(outputDir, fmt.Sprintf("%svalue%d.txt", keyHex, i+1))
			if err := os.WriteFile(textFileName, value, 0644); err != nil {
				log.Printf("Failed to write text value %d to file: %v", i+1, err)
			}
		}

		fmt.Printf("Saved value %d to %s\n", i+1, fileName)
	}
}
