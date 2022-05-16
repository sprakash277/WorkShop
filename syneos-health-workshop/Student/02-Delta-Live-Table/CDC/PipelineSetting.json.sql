-- Databricks notebook source
{
       "clusters": [
        {
            "label": "default",
            "policy_id": "606203597A000705",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5
            }
        },
        {
            "label": "maintenance",
            "policy_id": "606203597A000705"
        }
    ],
    "development": true,
    "continuous": false,
    "edition": "advanced",
    "photon": false,
    "libraries": [
        {
            "notebook": {
"path":"/Repos/DLT_PIPELINE_RUN/WorkShop/syneos-health-workshop/Instructor/02-Delta-Live-Table/CDC/2-Retail_DLT_CDC_sql"
            }
        }
    ],
    "name": "DLT_CDC",
    "configuration": {
        "source": "/tmp/demo/cdc_raw",
        "pipelines.applyChangesPreviewEnabled": "true"
    },
    "target": "DLT_EXERCISE"
}
