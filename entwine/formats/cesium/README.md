# Entwine 3D Tiles Output

## What's this?
For now, see instructions [here](https://github.com/connormanning/entwine-cesium-pages) to build for Cesium display.

## Batch tables
Specific point dimensions may be written to a tile's batch table. Only numeric dimensions are supported at present, and values
will be written to the binary portion of the batch table. To utilise this feature, add a "batchTable" property to the cesium
format configuration, taking an array of dimension names:

```json
"batchTable": [ "Dimension1", ... ]
```

## Other notes
For now, subset builds and continued build are not supported when building with Cesium output.  This will be available soon.

