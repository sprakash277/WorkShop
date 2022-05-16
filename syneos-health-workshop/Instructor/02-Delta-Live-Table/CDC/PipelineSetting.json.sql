-- Databricks notebook source
{
     "clusters": [
        {
            "label": "default",
            "num_workers": 1
        }
    ],
    "development": true,
    "continuous": false,
    "edition": "advanced",
    "photon": false,
    "libraries": [
        {
            "notebook": {
"path":"/Repos/mojgan.mazouchi@databricks.com/Delta-Live-Tables/notebooks/2-Retail_DLT_CDC_sql"
            }
        }
    ],
    "name": "DLT_CDC",
    "configuration": {
        "source": "/tmp/demo/cdc_raw",
        "pipelines.applyChangesPreviewEnabled": "true"
    },
    "target": "FILL_IN_THIS"
}

