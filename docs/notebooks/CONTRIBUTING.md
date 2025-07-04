# How to work with and contribute marimo wasm notebooks

## Basics

In this folder you will find a list of wasm based marimo notebooks. These run directly in your browser which has certain limitations: https://pyodide.org/en/stable/usage/wasm-constraints.html. Most notably threads and processes are not supported and wasm does not have sockets, so if you use any kind of networking, it will have to be http requests.

Currently we do not have a fully automated build system that picks up new notebooks. Please check the creation guide below to see which steps you will have to do to create a new notebook.

Notebooks are deployed to https://dlt-hub.github.io/dlt. There is no index page for now, so for example to see the playground, go to https://dlt-hub.github.io/dlt/playground/. You can also use this url for embedding the notebook in the docs.

Wasm notebooks install dependencies from pypi. This means you can not showcase any features that are unreleased to pypi. If you want to create a new notebook with a brand new feature, you will have to wait until it is release and then update the docs for the next release. In important cases we can consider merging something to master for a quicker notebook release.

## Available Make commands

* build: will build all notebooks as wasm projects into the build directory
* serve: will first run build and then start a webserver with all notebooks locally at localhost:9000. You can use this to test wether your notebooks run in wasm.
* test: runs all marimo notebooks in regular python, this is executed by ci and ensures that there are no easy to catch problems. There is a hidden test cell which does assertions at the end to make sure data was loaded or whatever your notebook does. We cannot test the wasm version of these notebooks, so this remains a manual step for now.

## Wasm notebook creation guide and checklist

If you want to create a new wasm notebook, please follow these steps:

* Copy the playground into a new folder, rename folder and python filename to your new notebook name
* Update the make commands in the make file. You need to update the build and test commands, you again can copy the playground commands and adapt them
* Update your notebooks code to what you need, you might need to change the installed dependencies that are fetched by micropip, and you need to update the assertions in the test cell.
* You can run the notebook directly in python to see that it executes properly. You should always build and test it in the browser too before you finish your work.
* After your branch was merged, you can see your notebook at the deployed url mentioned above. You can embed this notebook into the docs as an iframe, see how it is done in the playground.

Checklist for finalization:
* Dependencies with micropip up to date? You will see problems in the browser if not.
* All non-essential cells are hidden and don't take up screen real estate? Setup and testing cells should be hidden.
* Did you update the assertions in the test cell to account for all the things you are demonstrating? 
* Do a final check in the browser with a fresh build before you submit the notebook for review.