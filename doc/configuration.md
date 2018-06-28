# Entwine Configuration

Entwine provides 4 sub-commands for indexing point cloud data:

| Command             | Description                                                   |
|---------------------|---------------------------------------------------------------|
| [build](#build)     | Generate an EPT dataset from point cloud data                 |
| [scan](#scan)       | Aggregate information about point cloud data before building  |
| [merge](#merge)     | Merge datasets build as subsets                               |
| [convert](#convert) | Convert an EPT dataset to a different format                  |

These commands are invoked via the command line as:

    entwine <command> <arguments>

Although most options to entwine commands are configurable via command line,
each command accepts configuration via [JSON](https://www.json.org/).  A
configuration file may be specified with the `-c` command line argument.

Command line argument settings are applied in order, so earlier settings can be
overwritten by later-specified settings.  This includes configuration file
arguments, allowing them to be used as templates for common settings that may
be overwritten with command line options.

Internally, Entwine CLI invocation builds a JSON configuration that is passed
along to the corresponding sub-command, so CLI arguments and their equivalent
JSON configuration formats will be described for each command.  For example,
with configuration file `config.json`:

```json
{
    "input": "~/data/chicago.laz",
    "output": "~/entwine/chicago"
}
```

The following Entwine invocations are equivalent:

```
entwine build -i ~/data/chicago.laz -o ~/entwine/chicago
entwine build -c config.json
```

Throughout Entwine, a wide variety of point cloud formats are supported as input
data, as any [PDAL-readable](https://pdal.io/stages/readers.html) format may be
indexed.  Paths are not required to be local filesystem paths - they may be
local, S3, GCS, Dropbox, or any other
[Arbiter-readable](https://github.com/connormanning/arbiter) format.



## Build

The `build` command is used to generate an
[Entwine Point Tile](https://github.com/connormanning/ept) (EPT) dataset from
point cloud data.

| Key | Description |
|-----|-------------|
| [input](#input) | Path(s) to build |
| [output](#output) | Output directory |
| [tmp](#tmp) | Temporary directory |
| [reprojection](#reprojection) | Coordinate system reprojection |
| [threads](#threads) | Number of parallel threads |
| [force](#force) | Force a new build at this output |
| [dataType](#datatype) | Point cloud data storage type |
| [hierarchyType](#hierarchytype) | Hierarchy storage type |
| [ticks](#ticks) | Nominal resolution in one dimension |
| [allowOriginId](#alloworiginid) | Specify per-point source file tracking |
| [bounds](#bounds) | Dataset bounds |
| [schema](#schema) | Attributes to store |
| [trustHeaders](#trustheaders) | Specify whether file headers are trustworthy |
| [absolute](#absolute) | Set double precision spatial coordinates |
| [scale](#scale) | Scaling factor for scaled integral coordinates |
| [run](#run) | Insert a fixed number of files |
| [resetFiles](#resetfiles) | Reset memory pooling after a number of files |
| [subset](#subset) | Run a subset portion of a larger build |
| [overflowDepth](#overflowdepth) | Depth at which nodes may contain overflow |
| [overflowThreshold](#overflowthreshold) | Threshold for overflowing nodes to split |
| [hierarchyStep](#hierarchyStep) | Step size at which to split hierarchy files |

### input

The point cloud data paths to be indexed.  This may be a string, as in:
```json
{ "input": "~/data/autzen.laz" }
```

This string may be:
- a file path: `~/data/autzen.laz` or `s3://entwine.io/sample-data/red-rocks.laz`
- a directory (non-recursive): `~/data` or `~/data/*`
- a recursive directory: `~/data/**`
- a scan output path: `~/entwine/scans/autzen.json`

This field may also be a JSON array of multiples of each of the above strings:
```json
{ "input": ["autzen.laz", "~/data/"] }
```

Paths that do not contain PDAL-readable file extensions will be silently
ignored.

### output

A directory for Entwine to write its EPT output.  May be local or remote.

### tmp

A local directory for Entwine's temporary data.

### reprojection

Coordinate system reprojection specification.  Specified as a JSON object with
up to 3 keys.

If only the output projection is specified, then the input coordinate system
will be inferred from the file headers.  If no coordinate system information
can be found, this file will not be inserted.
```json
{ "reprojection": { "out": "EPSG:3857" } }
```

An input SRS may also be specified, which will be overridden by SRS information
determined from file headers.
```json
{
    "reprojection": {
        "in": "EPSG:26915",
        "out": "EPSG:3857"
    }
}
```

To force an input SRS that overrides any file header information, the `hammer`
key should be set to `true.
```json
{
    "reprojection": {
        "in": "EPSG:26915",
        "out": "EPSG:3857" ,
        "hammer": true
    }
}
```

### threads

Number of threads for parallelization.  By default, a third of these threads
will be allocated to point insertion and the rest will perform serialization
work.
```json
{ "threads": 9 }
```

This field may also be an array of two numbers explicitly setting the number of
worker threads and serialization threads, with the worker threads specified
first.
```json
{ "threads": [2, 7] }
```

### force

By default, if an Entwine index already exists at the `output` path, any new
files from the `input` will be added to the existing index.  To force a new
index instead, this field may be set to `true`.
```json
{ "force": true }
```

### dataType

Specification for the output storage type for point cloud data.  Currently
acceptable values are `laszip` and `binary`.  For a `binary` selection, data
is laid out according to the [schema](#schema).
```json
{ "dataType": "laszip" }
```

### hierarchyType

Specification for the hierarchy storage format.  Hierarchy information is
always stored as JSON, but this field may indicate compression.  Currently
acceptable values are `json` and `gzip`.
```json
{ "hierarchyType": "json" }
```

### ticks

The number of ticks in one dimension for the nominal grid size of the octree.
For example, a `ticks` value of `256` results in a `256 * 256 * 256` cubic
resolution.

### allowOriginId

For lossless capability, Entwine inserts an OriginId dimension tracking each
point back to their original source file.  If this value is present and set to
`false`, this behavior will be disabled.

### bounds

Total bounds for all points to be index.  These bounds are final, in that they
may not be expanded later after indexing has begun.  Typically this field does
not need to be supplied as it will be inferred from the data itself.  This field
is specified as an array of the format `[xmin, ymin, zmin, xmax, ymax, zmax]`.
```json
{ "bounds": [0, 500, 30, 800, 1300, 50] }
```

### schema

An array of objects representing the dimensions to be stored in the output.
Each dimension is specified with a string `name` and a string `type`.  Typically
this field does not need to be specified as it will be inferred from the data
itself.

Valid `type` values are: `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`,
`uint32`, `uint64`, `float`, and `double`.
```json
{
    "schema": [
        { "name": "X", "type": "uint32" },
        { "name": "Y", "type": "uint32" },
        { "name": "Z", "type": "uint32" },
        { "name": "Intensity", "type": "int8" }
    ]
}
```

### trustHeaders

By default, file headers for point cloud formats that contain information like
number of points and bounds are considered trustworthy.  If file headers are
known to be incorrect, this value can be set to `false` to require a deep scan
of all the points in each file.

### absolute

Scaled values at a fixed precision are preferred by Entwine (and required for
the `laszip` [dataType](#dataType)).  To use absolute double-precision values
for XYZ instead, this value may be set to `true`.

### scale

A scale factor for the spatial coordinates of the output.  An offset will be
determined automatically.  May be a number like `0.01`, or a 3-length array of
numbers for non-uniform scaling.
```json
{ "scale": 0.01 }
```
```json
{ "scale": [0.01, 0.01, 0.025] }
```

### run

If a build should not run to completion of all input files, a `run` count may be
specified to run a fixed maximum number of files.  The build may be continued
by providing the same `output` value to a later build.
```json
{ "run": 25 }
```

### resetFiles

For certain types of data or very long builds it may be useful to reset the
memory pool used by Entwine.  If this value is set, then after the specified
number of files, Entwine's memory pool will reset.
```
{ "resetFiles": 500 }
```

### subset

Entwine builds may be split into multiple subset tasks, and then be merged later
with the [merge](#merge) command.  Subset builds must contain exactly the same
configuration aside from this `subset` field.

Subsets are specified with a 1-based `id` for the task ID and an `of` key for
the total number of tasks.  The total number of tasks must be a power of 4.
```json
{ "subset": { "id": 1, "of": 16 } }
```

### overflowDepth

There may be performance benefits by not allowing nodes near the top of the
octree to contain overflow.  The depth at which overflow may begin is
specified by this parameter.

### overflowThreshold

For nodes at depths of at least the `overflowDepth`, this parameter specifies
the threshold at which they will split into bisected child nodes.

### hierarchyStep

For large datasets with lots of data files, the
[hierarchy](https://github.com/connormanning/entwine/blob/ept/doc/entwine-point-tile.md#hierarchy)
describing the octree layout is split up to avoid large downloads.  This value
describes the depth modulo at which hierarchy files are split up into child
files.  In general, this should be set only for testing purposes as Entwine will
heuristically determine a value if the output hierarchy is large enough to
warrant splitting.



## Scan

The `scan` command is used to aggregate information about unindexed point cloud
data prior to building an Entwine Point Tile dataset.

Most options here are common to `build` and perform exactly the same function in
the `scan` command, aside from `output`, described below.

| Key | Description |
|-----|-------------|
| [input](#input) | Path(s) to build |
| [output](#output-scan) | Output directory |
| [tmp](#tmp) | Temporary directory |
| [reprojection](#reprojection) | Coordinate system reprojection |
| [threads](#threads) | Number of parallel threads |
| [trustHeaders](#trustheaders) | Specify whether file headers are trustworthy |

### output (scan)

The `output` for a scan is a file path to write detailed summary information
determined by the scan, including per-file metadata.  This output file, in JSON
format, may be used as the `input` for a [build](#build) command.



## Merge

The `merge` command is used to combine [subset](#subset) builds into a full
Entwine Point Tile dataset.  All subsets must be completed.

| Key | Description |
|-----|-------------|
| [output](#output-merge) | Output directory of subsets |
| [tmp](#tmp) | Temporary directory |
| [threads](#threads) | Number of parallel threads |

### output (merge)

The output path must be a directory containing `n` completed subset builds,
where `n` is the `of` value from the subset specification.



## Convert

The `convert` command provides utilities to transform Entwine Point Tile output
into other formats.  Currently the only conversion provided is to the
[Cesium 3D Tiles](https://github.com/AnalyticalGraphicsInc/3d-tiles) format.
For proper positioning, data must be reprojected to `EPSG:3857` during the
`entwine build` step.

| Key | Description |
|-----|-------------|
| [input](#input-convert) | Directory containing a completed Entwine build |
| [output](#output-convert) | Output directory for the converted dataset |
| [tmp](#tmp) | Temporary directory |
| [threads](#threads) | Number of parallel threads |
| [colorType](#colorType) | Color selection for output tileset |
| [truncate](#truncate) | Truncate color values to one byte |
| [geometricErrorDivisor](#geometricerrordivisor) | Geometric error divisor |

### input (convert)

The `input` to a `convert` command is the path of a directory containing a
completed Entwine build.

### output (convert)

Output directory in which to write the converted dataset.

### colorType

An optional setting to select a coloring method for the output.  If omitted,
RGB will be used if they exist in the input, or Intensity if it exists and RGB
does not exist.  If neither exist and no `colorType` is provided, then the
output tileset will not contain color.

If set, valid values to color the RGB in the output are:

| Value | Description |
|-------|-------------|
| `none` | RGB is omitted |
| `rgb` | Color values from the input |
| `intensity` | Grayscale intensity values |
| `tile` | Each tile is randomly colored |

### truncate

Cesium accepts one-byte color values, but many formats allow two-byte storage
of intensities and RGB values.  If the input data contains values to be colored
as RGB values that are greater than `255`, then they may be scaled down to one
byte with the `--truncate` flag.

### geometricErrorDivisor

The root geometric error is determined as `cubeWidth / geometricErrorDivisor`,
which defaults to `32.0`.  Lower values will cause Cesium to render the data
more densely.
```json
{ "geometricErrorDivisor": 16.0 }
```

# Miscellaneous

## S3

Entwine can read and write S3 paths.  The simplest way to make use of this
functionality is to install [AWSCLI](https://aws.amazon.com/cli/) and run
`aws configure`, which will write credentials to `~/.aws`.

If you're using Docker, you'll need to map that directory as a volume.
Entwine's Docker container runs as user `root`, so that mapping is as simple as
adding `-v ~/.aws:/root/.aws` to your `docker run` invocation.

## Cesium

Creating 3D Tiles point cloud datasets for display in Cesium is a two-step
process.

First, an Entwine Point Tile datset must be created with an output projection of
earth-centered earth-fixed, i.e. `EPSG:4978`:

```
mkdir ~/entwine
docker run -it -v ~/entwine:/entwine connormanning/entwine:ept build \
    -i https://entwine.io/sample-data/autzen.laz \
    -o /entwine/autzen-ecef \
    -r EPSG:4978
```

Then, `entwine convert` must be run to create a 3D Tiles tileset:

```
docker run -it -v ~/entwine:/entwine connormanning/entwine:ept convert \
    -i /entwine/autzen-ecef \
    -o /entwine/cesium/autzen
```

Statically serve the tileset locally:

```
docker run -it -v ~/entwine/cesium:/var/www -p 8080:8080 \
    connormanning/http-server
```

And browse the tileset with
[Cesium](http://cesium.entwine.io/?url=http://localhost:8080/autzen).

