## 5. Literature Review

### 5.1 Enterprise Data Governance Frameworks

Traditional corporate data warehouses and centralized data lakes are highly centralized architectural patterns that are very common in the industry. They are useful because they bring storage, analytics, and governance into a single place, which makes it easier to join data across different business areas. However, this centralized model usually forces one central data team to understand every single data source and approve all schema changes. As the amount of data grows, this central team often turns into a major delivery bottleneck. Additionally, the data stored in centralized data lakes can easily lose its business context and clear ownership. As a result, data lakes can turn into collections of data that are accessible but poorly understood, while changes to a warehouse can cause unexpected problems for downstream users. Recent studies by Wider, Verma and Akhtar (2023) and Almaslukh *et al.* (2024) point out these functional limitations, showing that traditional centralized systems are often too rigid for modern needs.

Modern distributed frameworks, specifically Data Mesh and Data Contracts, try to solve these operational bottlenecks by shifting data ownership to the domain teams that actually understand the data, while still using a shared self-service platform. In this setup, data products offer discoverable data along with clear quality rules and policies (Wider, Verma and Akhtar, 2023). Data Contracts help enforce these rules by acting as machine-readable agreements about data schemas and sharing limits. As Wider, Jarmul and Akhtar (2024) explain, these contracts can be built into software delivery pipelines to test rules automatically. However, a local Data Contract cannot guarantee that the whole system is safe if there are hidden data copies or undocumented changes happening between the producer and consumer.

The lifecycle of these big data engineering pipelines is also heavily governed by modern regulations. When data is first ingested, it needs to be tagged with important details like its owner, purpose, and retention rules. This ensures it meets the General Data Protection Regulation (GDPR) rules for data minimization (European Parliament and Council, 2016) and follows the California Consumer Privacy Act (CCPA) (State of California, n.d.-a). Throughout the data's lifecycle, tracking end-to-end lineage is critical. This helps organizations know exactly who accessed the data and makes it possible to completely delete records if a user requests it, as required by law (State of California, n.d.-b).

Metadata catalog systems play a key operational role in managing this compliance by tracing automated data lineage and access controls. For example, Apache Atlas manages governance by tracking where data comes from and integrating with Apache Ranger to enforce strict access controls and data masking (Apache Software Foundation, 2020; 2023). OpenMetadata is another tool that maps out upstream and downstream table relationships, reads queries to automate lineage, and manages access policies (OpenMetadata, n.d.-a; n.d.-b; n.d.-c). However, these systems are not perfect; custom scripts or manual data transfers can create gaps in the lineage. Therefore, metadata catalogs should be used as control-plane tools, and their data always needs to be verified against what is actually happening in the runtime environment.

### 5.2 Emerging Frontiers – Quantum Computing & LLM-Driven Loop Engineering

#### Quantum Big Data Paradigms

Quantum Machine Learning (QML) and quantum-enhanced databases are exciting new areas of research that challenge standard constraints on NP-hard processing problems. In database engineering, tasks like multiple-query optimization and join ordering often involve NP-hard problems. However, just because a problem is hard for normal computers does not automatically mean a quantum computer will be useful for it. Studies by Haug, Self and Kim (2023) and Jerbi *et al.* (2023) show that QML can provide a speed advantage for specific tasks, but this does not prove that it can speed up standard enterprise pipelines.

In terms of databases, Fankhauser *et al.* (2023) successfully tested multiple-query optimization on a real quantum computer, but their experiments only handled very small datasets. Multidimensional indexing optimizations face similar limits. Trummer and Venturelli (2024) looked at optimizing index selection using quantum methods, while Zardini, Blanzieri and Pastorello (2024) explored distance estimation for search queries. These show promise for specific subproblems, but a complete, production-ready quantum index does not exist yet. Right now, the most significant operational disruption from quantum computing is the threat it poses to cryptographic signatures. Since future quantum computers could break the signatures used to secure data contracts and lineage events, data platforms must prepare to upgrade to NIST’s new quantum-resistant algorithms (National Institute of Standards and Technology, 2024a; 2024b).

#### LLM Loop Engineering

In the short term, Large Language Models (LLMs) are changing the field by enabling closed-loop data engineering cycles. Instead of letting LLMs run systems directly, they are being used as automated code-generation agents inside a strict loop: the LLM suggests the code, and then deterministic tools test it in a sandbox and validate it against Data Contracts before it is deployed.

This closed-loop approach is being widely researched for structural text-to-SQL layer synthesis. Papers on tools like SQLPrompt (Sun *et al.*, 2023) and DART-SQL (Mao *et al.*, 2024) demonstrate that using test results to guide the LLM makes the final queries much more accurate. The same idea is being applied to predictive pipeline optimization, such as Giannakouris and Trummer’s (2025) λ-Tune system for automated database tuning. Furthermore, LLMs could help with autonomous self-healing ETL errors; if a data pipeline breaks, the LLM can read the logs and propose a fix. However, to keep the system safe, these self-healing fixes must be strictly tested for accuracy before they are automatically promoted.

#### Open Synthesis

When putting these technologies together, a major system-level bottleneck occurs when integrating nondeterministic LLM outputs with strict, deterministic distributed runtime engines like Spark or Flink. Research by Ouyang *et al.* (2025) shows that LLMs frequently give different code answers for the exact same prompt, even on strict settings. This nondeterminism is a major issue because engines like Spark and Flink require explicit ordering, stable dependencies, and reliable logic to guarantee exactly-once processing.

To overcome this system-level bottleneck, anything generated by an LLM must be treated as untrusted. The generated code must be heavily tested, versioned, and cryptographically signed before it is allowed to interact with the runtime engine. Overall, while quantum computing and LLM tools offer powerful new ways to handle data, they can only be used safely if they are backed up by a strong, traditional governance framework that relies on Data Contracts and strict testing.

---

## References

Almaslukh, A., Alameer, A., Alsaleh, H., Alkadyan, F., Allheeib, N., Alhadlag, A. and Alabdulkarim, Y. (2024) ‘Data Mesh Meets Blockchain’, *International Journal of Computational Intelligence Systems*, 17, article 27. [https://doi.org/10.1007/s44196-024-00404-z](https://doi.org/10.1007/s44196-024-00404-z).

Apache Software Foundation (2020) *Apache Atlas: Hive model and hook*. Available at: [https://atlas.apache.org/2.0.0/Hook-Hive.html](https://atlas.apache.org/2.0.0/Hook-Hive.html) (Accessed: 10 August 2026).

Apache Software Foundation (2023) *Apache Atlas 2.3.0 documentation*. Available at: [https://atlas.apache.org/2.3.0/index.html](https://atlas.apache.org/2.3.0/index.html) (Accessed: 10 August 2026).

European Parliament and Council (2016) ‘Regulation (EU) 2016/679 of 27 April 2016 on the protection of natural persons with regard to the processing of personal data and on the free movement of such data’, *Official Journal of the European Union*, L119, pp. 1–88. Available at: [https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32016R0679](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32016R0679) (Accessed: 10 August 2026).

Fankhauser, T., Solèr, M.E., Füchslin, R.M. and Stockinger, K. (2023) ‘Multiple Query Optimization Using a Gate-Based Quantum Computer’, *IEEE Access*, 11, pp. 114031–114043. [https://doi.org/10.1109/ACCESS.2023.3324253](https://doi.org/10.1109/ACCESS.2023.3324253).

Giannakouris, V. and Trummer, I. (2025) ‘λ-Tune: Harnessing Large Language Models for Automated Database System Tuning’, *Proceedings of the ACM on Management of Data*, 3(1), article 2, pp. 1–26. [https://doi.org/10.1145/3709652](https://doi.org/10.1145/3709652).

Haug, T., Self, C.N. and Kim, M.S. (2023) ‘Quantum machine learning of large datasets using randomized measurements’, *Machine Learning: Science and Technology*, 4(1), 015005. [https://doi.org/10.1088/2632-2153/acb0b4](https://doi.org/10.1088/2632-2153/acb0b4).

Jerbi, S., Fiderer, L.J., Poulsen Nautrup, H., Kübler, J.M., Briegel, H.J. and Dunjko, V. (2023) ‘Quantum machine learning beyond kernel methods’, *Nature Communications*, 14, article 517. [https://doi.org/10.1038/s41467-023-36159-y](https://doi.org/10.1038/s41467-023-36159-y).

Mao, W., Wang, R., Guo, J., Zeng, J., Gao, C., Han, P. and Liu, C. (2024) ‘Enhancing Text-to-SQL Parsing through Question Rewriting and Execution-Guided Refinement’, in *Findings of the Association for Computational Linguistics: ACL 2024*. Bangkok: Association for Computational Linguistics, pp. 2009–2024. [https://doi.org/10.18653/v1/2024.findings-acl.120](https://doi.org/10.18653/v1/2024.findings-acl.120).

National Institute of Standards and Technology (2024a) *Digital signatures*. Available at: [https://csrc.nist.gov/Projects/digital-signatures](https://csrc.nist.gov/Projects/digital-signatures) (Accessed: 11 August 2026).

National Institute of Standards and Technology (2024b) *Module-Lattice-Based Digital Signature Standard*, FIPS 204. Available at: [https://csrc.nist.gov/pubs/fips/204/final](https://csrc.nist.gov/pubs/fips/204/final) (Accessed: 11 August 2026).

OpenMetadata (n.d.-a) *Lineage ingestion*. Available at: [https://docs.open-metadata.org/v1.12.x/connectors/ingestion/lineage](https://docs.open-metadata.org/v1.12.x/connectors/ingestion/lineage) (Accessed: 10 August 2026).

OpenMetadata (n.d.-b) *Glossary terms*. Available at: [https://docs.open-metadata.org/v1.12.x/how-to-guides/data-governance/glossary/glossary-term](https://docs.open-metadata.org/v1.12.x/how-to-guides/data-governance/glossary/glossary-term) (Accessed: 10 August 2026).

OpenMetadata (n.d.-c) *Advanced guide for roles and policies*. Available at: [https://docs.open-metadata.org/v1.12.x/how-to-guides/admin-guide/roles-policies](https://docs.open-metadata.org/v1.12.x/how-to-guides/admin-guide/roles-policies) (Accessed: 10 August 2026).

Ouyang, S., Zhang, J.M., Harman, M. and Wang, M. (2025) ‘An empirical study of the non-determinism of ChatGPT in code generation’, *ACM Transactions on Software Engineering and Methodology*, 34(2), article 42, pp. 1–28. [https://doi.org/10.1145/3697010](https://doi.org/10.1145/3697010).

State of California (n.d.-a) *California Civil Code § 1798.100*. Available at: [https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?lawCode=CIV&sectionNum=1798.100](https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?lawCode=CIV&sectionNum=1798.100) (Accessed: 10 August 2026).

State of California (n.d.-b) *California Civil Code § 1798.105*. Available at: [https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?lawCode=CIV&sectionNum=1798.105](https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?lawCode=CIV&sectionNum=1798.105) (Accessed: 10 August 2026).

Sun, R., Arik, S., Sinha, R., Nakhost, H., Dai, H., Yin, P. and Pfister, T. (2023) ‘SQLPrompt: In-context Text-to-SQL with minimal labeled data’, in *Findings of the Association for Computational Linguistics: EMNLP 2023*. Singapore: Association for Computational Linguistics, pp. 542–550. [https://doi.org/10.18653/v1/2023.findings-emnlp.39](https://doi.org/10.18653/v1/2023.findings-emnlp.39).

Trummer, I. and Venturelli, D. (2024) ‘Leveraging Quantum Computing for Database Index Selection’, in *Proceedings of the 1st Workshop on Quantum Computing and Quantum-Inspired Technology for Data-Intensive Systems and Applications (Q-Data '24)*. New York: Association for Computing Machinery, pp. 14–26. [https://doi.org/10.1145/3665225.3665445](https://doi.org/10.1145/3665225.3665445).

Wider, A., Jarmul, K. and Akhtar, A. (2024) ‘Towards automating federated data governance’, in *2024 IEEE International Conference on Web Services (ICWS)*. Shenzhen: IEEE, pp. 10–19. [https://doi.org/10.1109/ICWS62655.2024.00019](https://doi.org/10.1109/ICWS62655.2024.00019).

Wider, A., Verma, S. and Akhtar, A. (2023) ‘Decentralized data governance as part of a Data Mesh platform: Concepts and approaches’, in *2023 IEEE International Conference on Web Services (ICWS)*. Chicago, IL: IEEE, pp. 746–754. [https://doi.org/10.1109/ICWS60048.2023.00101](https://doi.org/10.1109/ICWS60048.2023.00101).

Zardini, E., Blanzieri, E. and Pastorello, D. (2024) ‘A quantum k-nearest neighbors algorithm based on the Euclidean distance estimation’, *Quantum Machine Intelligence*, 6, article 23. [https://doi.org/10.1007/s42484-024-00155-2](https://doi.org/10.1007/s42484-024-00155-2).
