# Entwine Point Tile

Entwine Point Tile (EPT) is a simple and flexible octree-based storage format for point cloud data.  This document is a working draft of the format.

The organization of and EPT dataset contains JSON metadata portions as well as binary point data.  This structure for a small dataset may look like this:

```
├── ept.json
├── ept-data
│   └── 0-0-0-0.laz
├── ept-hierarchy
│   └── 0-0-0-0.json
└── ept-sources
    ├── list.json
    └── 0.json
```

These files and directories are described below.

## ept.json

The core metadata required to interpret the contents of an EPT dataset.  An example file might look like this:
```json
{
    "bounds": [634962.0, 848881.0, -1818.0, 639620.0, 853539.0, 2840.0],
    "boundsConforming": [635577.0, 848882.0, 406.0, 639004.0, 853538.0, 616.0],
    "dataType": "laszip",
    "hierarchyType": "json",
    "points": 10653336,
    "schema": [
        { "name": "X", "type": "signed", "size": 4, "scale": 0.01, "offset": 637291.0 },
        { "name": "Y", "type": "signed", "size": 4, "scale": 0.01, "offset": 851210.0 },
        { "name": "Z", "type": "signed", "size": 4, "scale": 0.01, "offset": 511.0 },
        { "name": "Intensity", "type": "unsigned", "size": 2 },
        { "name": "ReturnNumber", "type": "unsigned", "size": 1 },
        { "name": "NumberOfReturns", "type": "unsigned", "size": 1 },
        { "name": "ScanDirectionFlag", "type": "unsigned", "size": 1 },
        { "name": "EdgeOfFlightLine", "type": "unsigned", "size": 1 },
        { "name": "Classification", "type": "unsigned", "size": 1 },
        { "name": "ScanAngleRank", "type": "float", "size": 4 },
        { "name": "UserData", "type": "unsigned", "size": 1 },
        { "name": "PointSourceId", "type": "unsigned", "size": 2 },
        { "name": "GpsTime", "type": "float", "size": 8 },
        { "name": "Red", "type": "unsigned", "size": 2 },
        { "name": "Green", "type": "unsigned", "size": 2 },
        { "name": "Blue", "type": "unsigned", "size": 2 },
        { "name": "OriginId", "type": "unsigned", "size": 4 }
    ],
    "span" : 256,
    "srs": {
        "authority": "EPSG",
        "horizontal": "3857",
        "vertical": "5703",
        "wkt": "PROJCS[\"WGS 84 ... AUTHORITY[\"EPSG\",\"3857\"]]"
    },
    "version" : "1.0.0"
}
```

### bounds
An array of 6 numbers of the format `[xmin, ymin, zmin, xmax, ymax, zmax]` describing the cubic bounds of the octree indexing structure.  This value is always in native coordinate space, so any `scale` or `offset` values will not have been applied.  This value is presented in the coordinate system matching the `srs` value.

### boundsConforming
An array of 6 numbers of the format `[xmin, ymin, zmin, xmax, ymax, zmax]` describing the narrowest bounds conforming to the maximal extents of the data.  This value is always in native coordinate space, so any `scale` or `offset` values will not have been applied.  This value is presented in the coordinate system matching the `srs` value.

### dataType
A string describing the binary encoding of the tiled point cloud data.  See the `Point cloud data` section.  Possible values:

- `laszip`: Point cloud files are [LASzip](https://laszip.org/) compressed, with file extension `.laz`.
- `binary`: Point cloud files are stored as uncompressed binary data in the format matching the `schema`, with file extension `.bin`.
- `zstandard`: Point cloud files are stored as compressed binary data (using [Zstandard](https://facebook.github.io/zstd/) compression) in the format matching the `schema`, with file extension `.zst`.

### hierarchyType
A string describing the encoding of the hierarchy information.  See the `Hierarchy` section.  The hierarchy itself is always represented as JSON, but this value may indicate a compression method for this JSON.  Possible values:

- `json`: Hierarchy is stored uncompressed with file extension `.json`.
- `gzip`: Hierarchy is stored as [Gzip](https://www.gnu.org/software/gzip/) compressed JSON with file extension `.json.gz`.

### points
A number indicating the total number of points indexed into this EPT dataset.

### schema
An array of objects that represent the indexed dimensions - every dimension has a `name`, `type`, and `size`.

Known dimension names are mapped to [PDAL well known dimension list](https://pdal.io/dimensions.html), but arbitrary names may exist in addition to these prescribed dimensions.  Sizes are specified in 8-bit bytes, and all dimension attributes must be of integral byte size.

There are 10 prescribed combinations of `type` and `size` representing primitive types available to most programming languages.

type | size | description
-----|------|------------
`signed` | `1` | `int8`
`signed` | `2` | `int16`
`signed` | `4` | `int32`
`signed` | `8` | `int64`
`unsigned` | `1` | `uint8`
`unsigned` | `2` | `uint16`
`unsigned` | `4` | `uint32`
`unsigned` | `8` | `uint64`
`float` | `4` | `float32`
`float` | `8` | `float64`

Attributes of other application-defined types (e.g. `string`) are possible as long as their `type` appropriately specifies their size in bytes.

For a `dataType` of `binary`, the `schema` provides information on the binary contents of each file.  However for `laszip` data, the file should be parsed according to its header, as individual [LASzip formats](https://www.asprs.org/wp-content/uploads/2010/12/LAS_1_4_r13.pdf) may combine dimension values.  For example, for point format IDs 0-4, `ReturnNumber`, `NumberOfReturns`, `ScanDirectionFlag`, and `EdgeOfFlightLine` dimensions are bit-packed into a single byte.

In addition to the required `name`, `type`, and `size` specifications, attributes may also contain optional `scale` and/or `offset` values.  These options specify that the absolutely positioned value of a given attribute should be computed as `read_value * scale + offset`.  If `scale` does not exist then it is implicitly `1.0`.  If `offset` does not exist it is implicitly `0.0`.  This is commonly used to provide a fixed precision to the `X`, `Y`, and `Z` spatial dimensions.

### span
This value represents the span of voxels in each spatial dimension defining the grid size used for the octree.  This value must be a power of `2`.

For example, a `span` of `256` means that the root volume of the octree consists of the `bounds` cube split into a voxelized resolution of `256 * 256 * 256`.

For aerial LiDAR data which tends to be much denser in its X and Y ranges than the Z range, for example, this would loosely correspond to a practical resolution of `256 * 256` points per volume.

Because the `span` is constant throughout an entire EPT dataset, but each subsequent depth bisects the cubic volume of the previous, each increase in depth effectively doubles the resolution in each spatial dimension.

### srs
An object describing the spatial reference system for this indexed dataset, or may not exist if a spatial reference could not be determined and was not set manually.  In this object there are string keys with string values of the following descriptions:
- `authority`: Typically `"EPSG"` (if present), this value represents the authority for horizontal code as well as the vertical code if one is present.
- `horizontal`: Horizontal coordinate system code with respect to the given `authority`.  If present, `authority` must exist.
- `vertical`: Vertical coordinate system code with respect to the given `authority`.  If present, both `authority` and `horizontal` must exist.
- `wkt`: A WKT specification of the spatial reference, if available.

For a valid `srs` specification: if one of either `authority` or `horizontal` exists, then the other must also exist.  If `vertical` exists, then both `authority` and `horizontal` must exist.

The `srs` specification may be an empty object.

### version
Version string in the form of `<major>.<minor>.<patch>`.

## ept-data
The point cloud data itself is arranged in a 3D analogous manner to [slippy map](https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames) tiling schemes.  The filename scheme `Zoom-X-Y` is expanded to three dimensions as `Depth-X-Y-Z`.  As opposed to raster tiling schemes where coarser-resolution data is replaced as its sub-tiles are traversed, the point cloud data is instead additive, meaning that full-resolution is obtained by the accumulated traversal from depth `0` to the last depth containing a positive point count for the selected area.

The root volume is always named `0-0-0-0`.  This volume always represents the volume of the cubic `bounds` value from `ept.json` split in each dimension by `span`.  So a `bounds` with a volume of 256 cubic meters, with a `span` value of `256`, corresponds to a root node with a resolution of up to 1 point per cubic meter.

Each node at depth `D` may have up to 8 child nodes at depth `D + 1` which represent bisected sub-volumes.  To zoom in, depth `D` is incremented by one, and each of `X`, `Y`, and `Z` are doubled and then possibly incremented by one.  Coordinates are treated as Cartesian during this bisection, so `X -> 2 * X` represents the sub-volume where the new maximum `X` value is the midpoint from its parent, and `X -> 2 * X + 1` represents the sub-volume where the new minimum `X` value is the midpoint from its parent.

In web-mercator, for example, where `X` increases going eastward and `Y` increases going northward, a traversal from `0-0-0-0` to `1-1-0-1` represents a traversal to the east/south/down sub-volume.

There is no fixed maximum resolution depth, instead the tiles must be traversed until no more data exists.  For look-ahead capability, see `ept-hierarchy`.

Note that unlike raster tiling schemes, where lower-resolution data is replaced by higher-resolution data during traversal, EPT is an additive scheme rather than a replacement scheme.  So data at lower resolution nodes is not duplicated at higher resolution nodes, and full resolution for a given area is found via the aggregation of all the points from the highest resolution leaf node with all overlapping lower resolution nodes.

## ept-hierarchy
The hierarchy section contains information about what nodes exist and how many points they contain.  The file format is simple JSON object, with string keys of `D-X-Y-Z` mapping to a point count for the corresponding file.  The root file of the hierarchy data exists at `ept-hierarchy/0-0-0-0.json`.  For example:
```json
{
    "0-0-0-0": 65341,
        "1-0-0-0": 438,
            "2-0-1-0": 322,
        "1-0-0-1": 56209,
            "2-0-1-2": 4332,
            "2-1-1-2": 20300,
            "2-1-1-3": 64020,
                "3-2-3-6": 32004,
                    "4-4-6-12": 1500,
                    "4-5-6-13": 2400,
                "3-3-3-7": 542,
        "1-0-1-0": 30390,
            "2-1-2-0": 2300,
        "1-1-1-1": 2303
}
```

Note that this sample is visually arranged hierarchically for clarity, which is not the case in practice.  In this example, the root node `0-0-0-0` contains 65341 points.  Nodes with zero points are never included in the hierarchy, so the absence of any child key indicates a leaf node (for example `4-4-6-12` above).

Hierarchy nodes must always contain a positive value with the sole exception of the sentinel value `-1`.  This value indicates that the number of points indicator value for this node resides in a subtree in its own file.  For example:


`ept-hierarchy/0-0-0-0.json`
```json
{
    "0-0-0-0": 65341,
        "1-0-0-0": 438,
            "2-0-1-0": 322,
        "1-0-0-1": 56209,
            "2-0-1-2": 4332,
            "2-1-1-2": 20300,
            "2-1-1-3": 64020,
                "3-2-3-6": -1,
                "3-3-3-7": -1,
        "1-0-1-0": 30390,
            "2-1-2-0": 2300,
        "1-1-1-1": 2303
}
```

`ept-hierarchy/3-2-3-6.json`
```json
{
    "3-2-3-6": 32004,
        "4-4-6-12": 1500,
        "4-5-6-13": 2400
}
```

`ept-hierarchy/3-3-3-7.json`
```json
{
    "3-3-3-7": 542
}
```

## ept-sources
Sparse input data source information is stored in an array at `ept-sources/list.json`.  This contains an array of JSON objects representing sparse metadata for each input source.  This array may potentially be an empty array if this information is not stored.  If an `OriginId` dimension exists in the `schema`, then each item's position in this array maps to its `OriginId` value in the EPT dataset, starting from `0` at the first position in the array.  An sample `list.json` file may look like this:

```json
[
    {
        "id": "not-a-point-cloud.txt"
    },
    {
        "id": "autzen-low.laz",
        "bounds": [635577.0, 848882.0, 406.0, 639004.0, 853538.0, 511.0],
        "url": "autzen.json"
    },
    {
        "id": "autzen-high.laz",
        "bounds": [635577.0, 848882.0, 511.0, 639004.0, 853538.0, 616.0],
        "url": "autzen.json"
    }
]
```

### id
For each object in the list, this is a field of string type which must uniquely identify this source among all other sources.  A typical `id` value would be a file path, but could potentially be something else like a data stream identifier or an arbitrary unique ID.

### bounds
A source object may optionally contain a bounds which, if existing, is an array of 6 numbers of the format `[xmin, ymin, zmin, xmax, ymax, zmax]`.

### url
A source object may optionally contain a string URL which points to a file, relative to the `ept-sources/` location, which contains more thorough metadata for this source.  If present, this URL must end in `.json` and this file must exist in JSON format.  Additionally, the JSON contained in this file must a) represent a JSON object and b) contain a string key matching the `id` of this source entry.  The format of this metadata file is discussed more fully in the *Source metadata* section, below.  More than one source entry may contain the same `url` string, and that file must contain an object with corresponding keys for each `id` that links to it.


### Source metadata
To be lossless and facilitate a full reconstitution of original source data files from an EPT index, the full metadata for each input file may be retained.  For each listed source in `ept-sources/list.json` containing a `url` path, this URL points to a JSON file containing the associated metadata for that source (and potentially other sources as well, discussed below).

Each of these files is a JSON object containing metadata information for one or more sources from `ept-sources/list.json`.  For each file, the JSON object must contain keys that match the `id` values from the source list, for each source entry pointing to this metadata file.

Within each of these ID sub-keys, an object must exist which may contain the previously-defined keys `bounds`, `points`, and `srs` which are defined in the `ept.json` section.  These keys have the same meanings and range of values described above, but are expressed here on a per-source basis rather than per-EPT dataset.

In addition to the well-known keys described above, each metadata object can also contain a `metadata` key, which maps to a JSON object representing arbitrary metadata for this source.

With the `ept-sources/list.json` file described above, a corresponding `ept-sources/autzen.json` file might look something like:

```json
{
    "autzen-low.laz": {
        "bounds": [635577.0, 848882.0, 406.0, 639004.0, 853538.0, 511.0],
        "points" : 180000,
        "srs" :
        {
            "authority" : "EPSG",
            "horizontal" : "3857",
            "wkt": "PROJCS[\"WGS 84 ... AUTHORITY[\"EPSG\",\"3857\"]]"
        },
        "metadata" : {
            "key": "value",
            "sofware_id": "PDAL",
            "version": 42,
            "something": "I am arbitrary metadata related to this source"
        }
    },
    "autzen-high.laz": {
        "bounds": [635577.0, 848882.0, 511.0, 639004.0, 853538.0, 616.0],
        "points" : 120000,
        "srs" :
        {
            "authority" : "EPSG",
            "horizontal" : "3857",
            "wkt": "PROJCS[\"WGS 84 ... AUTHORITY[\"EPSG\",\"3857\"]]"
        },
        "metadata" : {
            "key": "value",
            "sofware_id": "PDAL",
            "version": 58,
            "something_else": -1
        }
    }
}
```
