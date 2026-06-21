#!/bin/bash

if [[ ! -d "corpus" ]]; then
    echo "Building corpus..."
    mkdir corpus
    cp ../JSONTestSuite/test_parsing/* corpus
    cp ../jibbytests/* corpus
    cp seed_corpus/* corpus
else
    echo "Corpus already exists..."
fi

echo "Building fuzz file..."
go-fuzz-build -o jibbyfuzz.zip
