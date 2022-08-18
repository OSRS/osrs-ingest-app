# Optix Ingest App
Java application that handles data ingest, routing, transforms, and writing into the Optix.Time pipeline.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Optix Ingest App](#optix-ingest-app)
  - [Install](#install)
  - [Running](#running)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Install
`mvn clean install`

## Running
`java -cp "optix-ingest-app-<version>.jar" IngestApp`

Requires a local `config.json` to exist in the run directory or pass in the fully qualified path to an external file with:
`-DconfigFile=another/location/alt-config.json` 