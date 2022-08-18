# Sample Extensions
Example project which extends on the base functionality of the GIA jar.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Sample Extensions](#sample-extensions)
  - [Install](#install)
  - [Running](#running)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Install
`mvn clean install`

## Running
`java -cp "optix-ingest-app-<version>.jar:sample-extensions-<versions>.jar" IngestApp`

Requires a local `config.json` to exist in the run directory or pass in the fully qualified path to an external file with:
`-DconfigFile=another/location/alt-config.json` 