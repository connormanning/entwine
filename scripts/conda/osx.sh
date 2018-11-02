export CONDA_EXE=/Users/hobu/miniconda3/bin/conda
source /Users/hobu/miniconda3/bin/activate base


$CONDA_EXE activate pdal

#
BUILDDIR=conda-build

rm -rf $BUILDDIR
mkdir -p $BUILDDIR
cd $BUILDDIR

#CONFIG="Unix Makefiles"
CONFIG="Ninja"

export CC=${CONDA_PREFIX}/bin/clang
export CXX=${CONDA_PREFIX}/bin/clang++
export GDAL_HOME=${CONDA_PREFIX}
CC=$CC CXX=$CXX cmake -G "$CONFIG" -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_LIBRARY_PATH:FILEPATH="$CONDA_PREFIX/lib" \
            -DCMAKE_INCLUDE_PATH:FILEPATH="$CONDA_PREFIX/include" \
            -DCMAKE_INSTALL_PREFIX=${CONDA_PREFIX} \
            -DOPENSSL_ROOT_DIR=${CONDA_PREFIX} -DOPENSSL_INCLUDE_DIR=${CONDA_PREFIX}/include \
            ..
ninja

