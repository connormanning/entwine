# Entwine configuration

# Build

Although most common options to `entwine build` are fully configurable via command line, a configuration file may be used to specify settings as well.  This can be useful for configuration templating, maintaining build history, or for crafting complex configurations.

A configuration file will be loaded if the first parameter to `entwine build` is a file path.  This path may be local, HTTP, S3, Dropbox - or anything readable by [Arbiter](https://github.com/connormanning/arbiter).  This configuration file will provide overrides to any defaults specified by Entwine - so if your configuration only specifies one field, then the rest of the configuration fields need not be specified in the configuration.

Configuration settings are applied in the precedence order: Entwine defaults first, then configuration file settings, and finally command line settings.  This allows template configurations to be used for some settings but still makes them overridable via command line.

For example, a configuration file named `web-mercator.json` could be used to transform the output into `EPSG:3857` and set a default thread count:
```json
{
    "threads": 6,
    "reproject": { "out": "EPSG:3857" }
}
```

An invocation might look like:
`entwine build web-mercator.json -i ~/data/chicago -o ~/entwine/chicago`

Note that since command line flags take precedence over configuration file settings, we can use the same template to build with 9 threads, for example:
`entwine build web-mercator.json -i ~/data/chicago -o ~/entwine/chicago -t 9`

The only required fields are `input` and `output`, so a full build specification may be as simple as:
```bash
entwine build \
    -i https://entwine.io/sample-data/red-rocks.laz \
    -o ~/entwine/red-rocks
```

## Configuration fields

| Key               | Flag  | Type                      | Default   | Description
|-------------------|-------|---------------------------|-----------|-------------------
| `input`           | `-i`  | `String` or `[String]`    | None      | Path(s) to build [🔗](#input)
| `output`          | `-o`  | `String`                  | None      | Output directory [🔗](#output)
| `tmp`             | `-a`  | `String`                  | `"./tmp"` | Temporary directory [🔗](#tmp)
| `threads`         | `-t`  | `Number`                  | `9`       | Number of work threads [🔗](#threads)
| `reprojection`    | `-r`  | `Object`                  | None      | Coordinate system settings [🔗](#reprojection)
| `trustHeaders`    | `-x`<sup>\*</sup>| `Boolean`      | `true`    | `true` if file headers are accurate [🔗](#trust-headers)
| `force`           | `-f`<sup>\*</sup>| `Boolean`      | `false`   | `true` to overwrite previous build [🔗](#force)
| `prefixIds`       | `-p`<sup>\*</sup>| `Boolean`      | `false`   | If `true`, output files are randomly prefixed [🔗](#prefix-ids)
| `pointsPerChunk`  |       | `Number`                  | `262144`  | Points per chunk [🔗](#points-per-chunk)
| `numPointsHint`   |       | `Number`                  | Inferred  | Total number of points to be indexed [🔗](#number-of-points-hint)
| `bounds`          | `-b`  | `[Number]`                | Inferred  | Indexing bounds [🔗](#bounds)
| `schema`          |       | `Object`                  | Inferred  | Indexing dimensions [🔗](#schema)
| `arbiter`         |       | `Object`                  | None      | Arbiter configuration settings [🔗](#arbiter)
| `compress`        |       | `Boolean`                 | `true`    | True to compress output [🔗](#compress)
| `nullDepth`       |       | `Number`                  | `7`       | Tree depth to begin storing points [🔗](#tree-depths)
| `baseDepth`       |       | `Number`                  | `10`      | Tree depth for contiguous point storage [🔗](#tree-depths)
| `coldDepth`       |       | `Number`                  | None      | Maximum tree depth, or `null` for lossless [🔗](#tree-depths)
| `subset`          |       | `Object`                  | None      | Partial build specification [🔗](#subset)

\*: These are toggle flags.  The existence of this flag on the command line will inverse the default setting.  For example, `entwine build ~/config.json -x` will set `trustHeaders` to `false`.

## Field details

### Input
This field specifies the paths to be indexed.  A path may represent a file or the contents of a directory:
- A file: `~/data/autzen.laz`
- A non-recursive directory: `s3://lidar/iowa/` or `dropbox://connor/chicago/*`
- A recursively-traversed directory: `~/data/county-data/**`

Multiple paths may be specified as an array:

```json
{
    "input": [
        "~/data/iowa-city.laz",
        "~/data/des-moines.laz",
        "s3://iowa-lidar/county-data/*"
    ]
}
```

Paths that are not readable by [PDAL](http://pdal.io) will be ignored, so don't worry about extraneous files in a globbed directory.

| | |
|-----------|------------------------------------------------------------------
| Type      | `String` or `[String]`
| Default   | None - this field is *required*
| Flag      | `-i`
| Examples  | `-i ~/data/autzen.laz`, `-i s3://iowa-lidar/iowa/`, `-i "~/data/county-data/**"`

### Output
A directory for Entwine to write its output.  May be local or remote.

| | |
|-----------|------------------------------------------------------------------
| Type      | `String`
| Default   | None - this field is *required*
| Flag      | `-o`
| Examples  | `-o ~/entwine/autzen`, `-o s3://my-bucket/chicago`

### Tmp
A local directory for temporary data.  During processing of remote input paths, Entwine may write temporary files to this directory.

| | |
|-----------|------------------------------------------------------------------
| Type      | `String`
| Default   | `tmp`
| Flag      | `-a`
| Examples  | `-a /tmp`, `-a /opt/mnt`

### Threads
Number of worker threads for Entwine to use during processing.  Recommended to be no more than the number of physical cores on the machine.

| | |
|-----------|------------------------------------------------------------------
| Type      | `Number`
| Default   | `9`
| Flag      | `-t`
| Examples  | `-t 12`

### Reprojection
Coordinate system information.  There are three ways to specify this information.

- Only the output projection may be specified.  In this case, if coordinate system information cannot be found in a file, then the points from that file will not be inserted.
```json
{
    "reprojection": {
        "out": "EPSG:3857"
    }
}
```
```bash
-r EPSG:3857
```
- A default input projection may be specified, which will only be used if a file contains no coordinate system information.
```json
{
    "reprojection": {
        "in": "EPSG:26915",
        "out": "EPSG:3857"
    }
}
```
```bash
-r EPSG:26915 EPSG:3857
```
- An input projection may be explicitly forced, which overrides any file header information.
```json
{
    "reprojection": {
        "in": "EPSG:26915",
        "out": "EPSG:3857",
        "hammer": true
    }
}
```
```bash
-r EPSG:26915 EPSG:3857 -h
```

### Trust headers
If true, file headers are considered accurate for bounds information and point count.  This information is used for a few things:
- Filling in the `bounds` configuration entry if it is not provided
- Filling in the `numPointsHint` entry if it is not provided
- During build-time, determining whether a given file should be inserted based on its bounds

Setting this field to false typically has an enormous impact on indexing time, and it is highly recommended to correct any false header information before indexing time so that this field may be set to `true`.

This is a toggle flag, so it may be omitted unless it is to be set to the non-default value of `false`.

| | |
|-----------|------------------------------------------------------------------
| Type      | `Boolean`
| Default   | `true`
| Flag      | `-x`
| Examples  | `entwine build -i ... -o ... -x`

### Force
In general, if an Entwine index already exists at the `output` path, any new files from the `input` will be added to the existing index.  To change this behavior to overwrite the existing index, the `force` flag may be set to true.

This is a toggle flag, so it may be omitted unless it is to be set to the non-default value of `true`.

| | |
|-----------|------------------------------------------------------------------
| Type      | `Boolean`
| Default   | `true`
| Flag      | `-f`
| Examples  | `entwine build -i ... -o ... -f`

### Prefix IDs
If set to `true`, Entwine's output keys will be prefixed with a hash.  For distributed filesystems sharded by key (namely S3), this can significantly increase throughput by reducing the number of failed writes, which must be retried.

This is a toggle flag, so it may be omitted unless it is to be set to the non-default value of `true`.

| | |
|-----------|------------------------------------------------------------------
| Type      | `Boolean`
| Default   | `false`
| Flag      | `-p`
| Examples  | `entwine build -i ... -o ... -p`

### Points per chunk
The base number of points per chunk, in two dimensions, starting at a depth of `baseDepth`.  For example, a `baseDepth` of `10` means that the first depth beyond the base contains up to `4^10 = 1,048,576` points.  With a `pointsPerChunk` value of `262,144`, this depth would be split into a maximum of 4 chunks.  This field must be set to a power of 4.

| | |
|-----------|------------------------------------------------------------------
| Type      | `Number`
| Default   | `262144`

### Number of points hint
A hint at the total number of points that will be indexed to this `output` location.  Precise accuracy is not very important - values within approximately a power of 4 in either direction of the true value are sufficient.  This value allows Entwine to make some rough inferences about tree depths, which allow for some speed and space optimizations.

| | |
|-----------|------------------------------------------------------------------
| Type      | `Number`
| Default   | None (inferred from input files)

### Bounds
Total bounds for all points to be indexed.  This may not be expanded after indexing has begun, so if a build will have data added after the initial build step, that new data must remain within the initially specified bounds (out of bounds data will be discarded).  If a reprojection is set, then the bounds should be specified in the output projection.

This field is specified as `[xmin, ymin, zmin, xmax, ymax, xmax]`.

| | |
|-----------|------------------------------------------------------------------
| Type      | `Boolean`
| Default   | None (inferred from input files)
| Flag      | `-b`
| Examples  | `entwine build -i ... -o ... -b "[0, 500, 30, 800, 1300, 50]`

### Schema
An object representing the dimensions to be stored in the output index.  Each dimension is specified with a name, type, and size.

Generally, this field can be omitted and will be inferred from the input files.  However, to omit or reorder dimensions, the schema may be used to specify this.  A sample schema might look like this:
```json
{
    "schema": {
        { "name": "X", "type": "floating", "size": 8 }
        { "name": "Y", "type": "floating", "size": 8 },
        { "name": "Z", "type": "floating", "size": 8 },
        { "name": "Intensity", "type": unsigned, "size": 1 }
    }
}
```

The `name` of a dimension maps directly to a [PDAL](https://pdal.io) dimension name.  The `type` and `size` fields specify the representation of the dimension.  Sizes are always specified in 8-bit bytes.

| Type          | Description                       | Available sizes
|---------------|-------------------------------------------------------
| Floating      | A floating point representation   | `4`, `8`
| Unsigned      | Unsigned integer representation   | `1`, `2`, `4`, `8`
| Signed        | Signed integer representation     | `1`, `2`, `4`, `8`

### Arbiter
Entwine uses [Arbiter](http://arbitercpp.com) ([source repository](https://github.com/connormanning/arbiter)) for reading and writing.  Arbiter provides abstracted access to resources like the local filesystem, S3, HTTP, and Dropbox.  Arbiter accepts a JSON configuration which may include things like verbosity settings and access tokens for S3 or Dropbox:

```json
{
    "arbiter": {
        "dropbox": { "token": "Kx17tHPzGIoAAAAA..." }
    }
}
```

For S3, if AWSCLI has been installed, then the credentials will be picked up from there, making this field unnecessary.

### Compress
Determines whether point data is compressed via [LAZ-perf](https://github.com/hobu/laz-perf).  In general, turning off compression is not very useful except for special cases of debugging or validating output.

### Tree depths
The three configurable depth settings correspond to depths of an octree.  Within this section, we will refer to LiDAR data, which tends to be a horizontal-ish "slice" of elevation across a somewhat consistent density across the X-Y bounds.  That means that even though the index is an octree, it tends to act as a quadtree.  This assumption will drive some of the heuristic values mentioned in this section.  For data that is as uniformly dense across Z as it is across X-Y, this behavior will be different - but here will we refer to quadtrees for conceptual simplicity.

The `nullDepth` refers to the maximum tree depth which is left empty at the top of the tree.  In a traditional quadtree, depth `0`, a.k.a. the root node, would contain a single point.  Depth `1` would contain a maximum of 4 points, one in each quadrant of the overall bounds, and this splitting would continue recursively.  Because data at these levels tends not to be very useful, a reasonable `nullDepth` can increase throughput (fewer comparisons overall) without losing meaningful structure.  A `nullDepth` of `8` means that depths `[0, 8)` contain no points, so the base "slice" of the tree would contain a maximum of `4^8`, or `65536` points.  The data at this depth would look like a gridded quantization of `256 x 256` cells.

The `baseDepth` represents the tree depths (after the `nullDepth`) which will be maintained in contiguous memory throughout the duration of a build.  A `nullDepth` of `6` and `baseDepth` of `10` means that `4^6 + 4^7 + 4^8 + 4^9 = 348160` point slots will be held in contiguous.

The `coldDepth` defines the maximum tree depth.  Typically this should be set to `null` or omitted to create a lossless build, but for debugging or purposely lossy low-resolution builds this field may be useful.  Points beyond the `baseDepth` are not maintained in memory when they are not needed, so they are periodically serialized during the build.  They are allocated in blocks of size `pointsPerChunk`.  As a numerical example, considering our earlier `baseDepth` of `10` (which is exclusive to `10`), then the first depth beyond this base depth contains `4^10 = 1,048,576` points.  With a `pointsPerChunk` of `262,144`, this depth would consist of 4 chunks, which may be created, re-created, and serialized on-demand as the build progresses.

### Subset
Builds may be split into multiple subsets, or tasks, and then be merged later.  This may be useful for EC2/S3 tasking, which allows multiple machines to be leveraged at the same time for the same build.  Subset builds must each contain the *same* configuration aside from the `subset` specification, including the same `input` (even if certain files from the input will not be processed by a given subset) and same `output` directory.  Subsets are split geographically, so a 4-subset build will split the overall bounds into 4 quadrants.  Subsets are specified as follows:

```json
{
    "subset": {
        "id": 1,
        "of": 4
    "
}
```

The above represents a single quadrant of 4 total.  `subset.of` must be a binary power, and `subset.id` values are 1-based.  For the above example, the same configuration would need to be run with `subset.id` values of `2`, `3`, and `4` to complete the pre-merge work.

Subset builds may also be specified via the command line, using the `-s` flag, which is usually simpler than making many variations of the same configuration file.  This flag uses the form `-s <id> <of>`.  For example:

```bash
entwine build ~/template.json -s 1 2
entwine build ~/template.json -s 2 2
```

This example shows the use of a common configuration and two different subsets.  Other fields may also be specified on the command line, but the resulting configurations must be equivalent (aside from the `subset` specification).

Let's take a sample dataset with 4 files contained in 4 quadrants, `northeast.laz`, `northwest.laz`, `southeast.laz`, and `southwest.laz`.  Assume that they should be built in 4 subsets on different machines.  Although a single build will only insert one of these files, *all four paths must be present in the `input` specification* for each build.  This is so Entwine can properly keep track of which points come from which file.  A sample configuration file for this build might look like this:
```json
{
    "input": [
        "s3://input/northeast.laz", "s3://input/northwest.laz",
        "s3://input/southeast.laz", "s3://input/southwest.laz"
    ],
    "output": "s3://my-organization/entwine/quadrants"
}
```

If that configuration were stored at `s3://my-organization/configurations/quadrants`, the build steps would then look like:
```bash
entwine build s3://my-organization/configurations/quadrants -s 1 4
entwine build s3://my-organization/configurations/quadrants -s 2 4
entwine build s3://my-organization/configurations/quadrants -s 3 4
entwine build s3://my-organization/configurations/quadrants -s 4 4
```

When all subsets are complete, the full build is not accessible until [entwine merge](#merge) is successfully run.

# Merge
The `merge` command is used to merge completed subset builds.  It takes a directory path as its argument, which corresponds to the `output` value provided to each of the `build` steps.  For example:

```bash
entwine merge s3://my-organization/entwine/quadrants
```

# Infer
This command is used to determine information about an unindexed dataset.  It may be used to ensure that file headers and reprojected coordinates look reasonable before running an actual `build`.  If, during a `build`, any of the `bounds`, `schema`, or `numPointsHint` fields are omitted, this step will be run internally before the start of the data processing.  As long as `trustHeaders` is `true` (which is the default), this step is very lightweight and fast, and will not perform downloads of the full dataset.  However, this step is very expensive if `trustHeaders` is `false`, so it is highly recommended that any incorrect headers be patched to contain accurate information.

The `infer` command does not an accept a configuration file, but accepts several of the same command line arguments as `build`.  The first argument to `infer` represents the path to read.  The results from this command is some logging containing information about the dataset.  Detailed per-file information can be written to an output file with the `-o` flag.

## Inference flags

| Flag  | Type                      | Default   | Description
|-------|---------------------------|-----------|-------------------
| `-a`  | `String`                  | `"./tmp"` | Temporary directory [🔗](#tmp)
| `-t`  | `Number`                  | `9`       | Number of work threads [🔗](#threads)
| `-r`  | `Object`                  | None      | Coordinate system settings [🔗](#reprojection)
| `-x`<sup>\*</sup>| `Boolean`      | `true`    | `true` if file headers are accurate [🔗](#trust-headers)
| `-o`  | `String`                  | None      | File path for per-file JSON output[🔗](#inference-output-path)

\*: These are toggle flags.  The existence of this flag on the command line will inverse the default setting.  For example, `entwine build ~/config.json -x` will set `trustHeaders` to `false`.

Sample invocations:
```bash
entwine infer ~/data/bridge.laz
entwine infer s3://bucket/iowa/** -r EPSG:26915 EPSG:3857 -h -t 12
entwine infer ~/data/counties/ -r EPSG:3857 -o ~/data/county-inference.json
```

### Inference output path
The standard operation of `infer` logs information to STDOUT.  If the `-o` flag is present, per-file information including bounds and number of points will be written to this output file in JSON format.

