## Examples of unit testing of Spark applications

This directory contains examples of the code for unit testing of Spark applications.


### Scala

Tests could be executed via `mvn test` (you may get some warnings about already running Spark sessions), or individually, with `mvn test -Dsuites=*TestName` (the `*` is important!).

Examples:
* [TestSparkTestingBase](src/test/scala/net/alexott/demos/spark/TestSparkTestingBase.scala) - demonstrates use of [spark-testing-base](https://github.com/holdenk/spark-testing-base)
* [TestSparkSuite.scala](src/test/scala/net/alexott/demos/spark/TestSparkSuite.scala) - demonstrates use of the Spark's built-in test framework
* [TestSparkFastTests.scala](src/test/scala/net/alexott/demos/spark/TestSparkFastTests.scala) - demonstrates use of [spark-fast-tests](https://github.com/MrPowers/spark-fast-tests).


### Python

Install necessary dependencies with: `pip install -r requirements.txt`

Examples:
* [test-pytest-spark.py](src/python/test-pytest-spark.py) - demonstrates use of [pytest-spark](https://github.com/malexer/pytest-spark).  Run with `pytest src/python/test-pytest-spark.py`
* [test-chispa.py](src/python/test-chispa.py) - demonstrates use of [chispa](https://github.com/MrPowers/chispa).  Run with `pytest src/python/test-chispa.py`
* [test-spark-testing-base.py](src/python/test-spark-testing-base.py) - demonstrates use of [spark-testing-base](https://github.com/holdenk/spark-testing-base).  Run with `python src/python/test-spark-testing-base.py`
* [test-delta.py](src/python/test-delta.py) - demonstrates use of [pytest-spark](https://github.com/malexer/pytest-spark) to setup the Delta for testing.  Run with `pytest src/python/test-delta.py`
