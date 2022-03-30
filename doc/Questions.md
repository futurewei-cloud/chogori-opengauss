# Questions about OpenGauss

1. Multi-thread model: OpenGauss used multi-thread model for client connections and internal processing, which is different from Postgres's forking child mechanism
  - Wat is the architecture of this model?
  - How are client connections handled?
  - How is the database session handled?
  - How does the worker process the client requests

2. New features: OpenGauss introduced a lot of new features into the original Postgres 9.2
  - How is the high performance (for example, 2-way Kunpeng 128 core 1.5 million TPMC.) achieved?
  - How are key data structures shared by internal threads are divided into different partitions?
  - How is UMA Structure used?
  - How is core-binding achieved?
  - How is SQL bypass implemented?

3. The Memory-Optimized Tables (MOT)
  - MOT architecture overview
  - Seems MOT is embedded inside OpenGauss, does it support distributed partitions?
  - How is MOT integrated with OpenGauss?
  - Future plan for MOT? for example, move MOT outside of OpenGauss? make it distributed if it is not already?
  - Any reason that we cannot achieve the 2.5x – 4x higher TPCC throughput in our own testing?

4. Postgres XL: seems OpenGauss incorporated Postgres XL project and introduced GTM, data nodes, coordinator nodes, and so on to achieve both write-scalability and massively parallel processing
  - Which verion of Postgre XL was ported into OpenGauss
  - Is the Postgres XL fully functional?
  - Has the Postgres XL been used in real production?

5. AI/Machine Learning
  - How is the SQL execution time prediction implemented?
  - How is the diagnoser for SQL execution statements implemented?
  - How is the database parameter adjustment achieved?
  - What data/metrics in OpenGauss are collected by the AI/ML engines?
  - What if we disable all the AI/ML related features in OpenGauss? Would the system still work fine?

6. Background Services/daemons
  - How does the job scheduler work?
  - How does the background writer bgwriter work?
  - How does the page writer pagewriter work?
  - How does the background worker bgworker work?

7. JDBC/ODBC
  - Why OpenGauss need additional project for JDBC/ODBC instead of using the one from Postgres directly?
  - What new features are supported in the JDBC/ODBC driver?
