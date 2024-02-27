#!/usr/bin/env bash
set -euo pipefail

JSON_FILE="${1?missing arg}"

exec 1> >(skyscope import-graphviz)

echo "digraph G {"

#<"$JSON_FILE" jq -r '
#    "  \"" + .name + "\" [ label=\"" + .name + "\\n" + .attribute_path + "\" ];"
#'
#<"$JSON_FILE" jq -r '
#    "  \"" + .build_inputs[].output_path + "\" -> \"" + .name + "\";"
#'
#<"$JSON_FILE" jq -r '
#    "  \"" + .name + "\" -> \"" + .outputs[].output_path + "\";"
#'

<"$JSON_FILE" jq -r '
    "  \"" + .output_path + "\" [ label=\"" + .name + "\\n" + .output_path + "\" ];"
'
<"$JSON_FILE" jq -r '
    "  \"" + .build_inputs[].output_path + "\" -> \"" + .output_path + "\";"
'
#<"$JSON_FILE" jq -r '
#    "  \"" + .name + "\" -> \"" + .outputs[].output_path + "\";"
#'


echo "}"

