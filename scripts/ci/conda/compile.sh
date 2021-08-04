#!/bin/bash

mkdir packages


CI_SUPPORT=""
if [ "$RUNNER_OS" == "Windows" ]; then
    CI_SUPPORT="win_64_.yaml"
fi

if [ "$RUNNER_OS" == "Linux" ]; then
    CI_SUPPORT="linux_64_.yaml"
fi

if [ "$RUNNER_OS" == "macOS" ]; then
    CI_SUPPORT="osx_64_.yaml"
fi

conda build recipe --clobber-file recipe/recipe_clobber.yaml --output-folder packages -m .ci_support/$CI_SUPPORT

conda install -c ./packages entwine

entwine --version
