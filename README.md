# Klaw ClusterApi

Klaw is a self-service Apache Kafka Topic Management tool/portal. It is a web application which automates the process of creating and browsing Kafka topics, acls, avro schemas, connectors by introducing roles/authorizations to users of various teams of an organization.
Klaw ClusterApi application : it is required to run Klaw https://github.com/aiven/klaw before running this api.

This cluster api communicates with Kafka brokers(through AdminClientApi) or other relevant Api and connects to UserInterfaceApi (klaw).

## Built With

* [Maven](https://maven.apache.org/) - Dependency Management
* Java, Spring boot, Spring security, Kafka Admin client

## Versioning

For the versions available, see the [tags on this repository](https://github.com/aiven/klaw-cluster-api/tags).

## Architecture:

![Architecture](https://github.com/aiven/klaw/blob/main/arch.png)

## Install

mvn clean install
Follow steps at https://klaw-project.io/docs

## License

Klaw is licensed under the Apache license, version 2.0.  Full license text is
available in the [LICENSE.md](LICENSE.md) file.

Please note that the project explicitly does not require a CLA (Contributor
License Agreement) from its contributors.

## Contact

Bug reports and patches are very welcome, please post them as GitHub issues
and pull requests at https://github.com/aiven/klaw-cluster-api . Any possible
vulnerabilities or other serious issues should be reported directly to the
maintainers <opensource@aiven.io>.

## Trademark

Apache Kafka is either a registered trademark or trademark of the Apache Software Foundation in the United States and/or other countries.
All product and service names used in this page are for identification purposes only and do not imply endorsement.

## Credits

Klaw ClusterApi (formerly Kafkawize) is maintained by, Aiven_ open source developers.


.. _`Aiven`: https://aiven.io/

Recent contributors are listed on the GitHub project page,
https://github.com/aiven/klaw/graphs/contributors

Copyright (c) 2022 Aiven Oy and klaw project contributors.