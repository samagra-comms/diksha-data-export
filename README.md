# diksha-data-export
###  List of Tables
* `job-request`
* `response`

### List of DAGs
* Request creator
* uci-response-exhaust
* CSV Process


### Implemenatation Details
* A config file `exhaust_config.py` will contain an array of 'config' objects. The config object will have this structure:

    ```json
    {
        "state_id": "1",
        "state_token": "xad1231va",
        "db_credentials": {
            "uri": "vault.uci.exhaust.db"
        }
    }
    ```

* We will have a table `cron_config` with the structure containing runtime config. `frequency` will be of [crontab format](https://crontab.guru/). CSV Process job dumps CSV to DB using bulk insert APIs.

    | state_id | bot_id | frequency | job-type |
    |--------- | ------ | -------- | ------- |
    | 1 | 1 | * * * * * | uci-response-exhaust |
    | 2 | 2 | 0 0 * * SUN| uci-private-exhaust |
    | 2 | 2 | 0 0 * * SUN| csv-process |


* We will have a DAG `exhaust_requester` that will run based on the `cron_config` above
    - which will first fetch the config from both the `exhaust_config.py` file and `exhaust_config` table.
    - then join the result using `state_id`.
    - then for each result object we will push request which need to be sent into table `job_request`.
    - after successful response the data is saved in a CSV on minio.

    `tag` will be auto generated UUID, `start_date` & `end_date` will get inferred from frequency.

    `status` can be `NULL`, `SUBMITTED`, `SUCCESS`, `MANUAL ABORT` and `ERROR`.

    | bot | tag | start_date | end_date | status | state_id | dataset | request_id | csv | job-type |
    | ------ | --- | ---------- | -------- | ------ | -------- | ------- | ---------- | --- |--- |
    | 1 | 1 | 28/06/2022 | 29/06/2022 | `null` | 1 | `uci-response-exhaust` | null | null | uci-response-exhaust |
    | 2 | 2 | 27/06/2022 | 28/06/2022 | SUBMITTED | 1 | `uci-private-exhaust` | x12esa1Asad | http://cdn.samagra.io/x.csv | uci-private-exhaust |
    | 2 | 2 | 27/06/2022 | 28/06/2022 | SUBMITTED | 1 | `uci-private-exhaust` | x12esa1Asad | http://cdn.samagra.io/x.csv | csv-process |

        * Next we will send the request to `{{host}}/dataset/v1/request/submit` by pick requests from `exhaust_requests` table whose `end_date` is lesser than or equals to current date. At last, we will update the `request_id` and `status` we get while making the above request into the `exhaust_requests` table.

        * We will have another DAG `exhaust_request_handler` that will run in the evening which will pick all the pending requests meaning whose `request_id` is not null from the `exhaust_requests` table and send the request to `{{host}}/dataset/v1/request/read` with the all the required body data which we are getting from `exhaust_requests` table. If the request was successful we will update the `status` of that request tuple to `complete` and download the CSV file and parse it to store it into `exhaust_report_response` or `exhaust_report_private` depending on the `dataset` value.
