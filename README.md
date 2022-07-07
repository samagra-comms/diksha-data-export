# diksha-data-export

# Action Plan for Exhaust Report

* A config file `exhaust_config.py` will contain an array of 'config' objects. The config object will have this structure:

    ```
    {
        state_id: "1",
        state_token: "xad1231va",
        db_credentials: {
            uri: "vault.uci.exhaust.db"
        }
    }
    ```

* We will have a table `cron_config` with the structure containing runtime config. `frequency` will be of [crontab format](https://crontab.guru/).

    | state_id | bot_id | frequency | dataset |
    |--------- | ------ | -------- | ------- |
    | 1 | 1 | * * * * * | uci-response-exhaust |
    | 2 | 2 | 0 0 * * SUN| uci-private-exhaust |


* We will have a DAG `exhaust_requester` that will run in the morning which will first fetch the config from both the `exhaust_config.py` file and `exhaust_config` table then join the result using `state_id`, then for each result object we will push request which need to be sent into table `exhaust_requests` table structure would be:

    | bot_id | tag | start_date | end_date | status | state_id | dataset | request_id |
    | ------ | --- | ---------- | -------- | ------ | -------- | ------- | ---------- |
    | 1 | 1 | 28/06/2022 | 29/06/2022 | `null` | 1 | `uci-response-exhaust` | null |
    | 2 | 2 | 27/06/2022 | 28/06/2022 | SUBMITTED | 1 | `uci-response-exhaust` | x12esa1Asad |


    `tag` will be auto generated UUID, `start_date` & `end_date` will get inferred from frequency.


* Next we will send the request to `{{host}}/dataset/v1/request/submit` by pick requests from `exhaust_requests` table whose `end_date` is lesser than or equals to current date. At last, we will update the `request_id` and `status` we get while making the above request into the `exhaust_requests` table.

* We will have another DAG `exhaust_request_handler` that will run in the evening which will pick all the pending requests meaning whose `request_id` is not null from the `exhaust_requests` table and send the request to `{{host}}/dataset/v1/request/read` with the all the required body data which we are getting from `exhaust_requests` table. If the request was successful we will update the `status` of that request tuple to `complete` and download the CSV file and parse it to store it into `exhaust_report_response` or `exhaust_report_private` depending on the `dataset` value.
