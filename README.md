# flink-network-traffic-analysis

- A Flink application project using Scala and SBT for Network Traffic Analysis.

- In order to run your application from within IntelliJ, open the Configuration drop-down and click on Edit Configuration and set the output file path in Program Arguments.

- Output file path = location where the output of the analysis will be dumped into as a text file.

- Required input file (Dataset) in present in the "resources" directory.

- Structure of input data: ## user_id, network_name, user_IP, user_country, website, time spent before next click

- Use case -

    For every 10 second find out for US country -

    a.) total number of clicks on every website in separate file

    b.) the website with maximum number of clicks in separate file.

    c.) the website with minimum number of clicks in separate file.

    d.) Calculate number of distinct users on every website in separate file.

    e.) Calculate the average time spent on website by users.
