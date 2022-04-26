# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Include the code that we'll test

# COMMAND ----------

# MAGIC %pip install unittest-xml-reporting

# COMMAND ----------

# MAGIC %run "./Library Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Import package for unit testing
# MAGIC 
# MAGIC We'll use built-in package: [unittest](https://docs.python.org/3/library/unittest.html).

# COMMAND ----------

import unittest

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Tests could be implemented as classes
# MAGIC 
# MAGIC One of the main advantages of implementing as classes is that we can execute some functions before/after our test functions by implementing the `setUp()`, `tearDown()`, etc.
# MAGIC 
# MAGIC Another advantage that we'll inhering many useful functions such as, `assertEqual`, `assertIsNotNone`, ..., and we can annotate the function & class with additional information, for example, if failure is expected.

# COMMAND ----------

class SimpleTest(unittest.TestCase):
    def test_data_generation(self):
      n = 100
      name = "tmp42"
      generate_data(n=n, name=name)
      df = spark.sql(f"select * from {name}")
      self.assertEqual(df.count(), n)

    def test_data_prediction(self):
      predicted = get_data_prediction()
      self.assertEqual(predicted, 42)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Tests could be implemented as functions
# MAGIC 
# MAGIC This is mostly done when we already have some code that performs testing, and we want to integrate it into the testing pipeline.

# COMMAND ----------

def test_data_generation():
  n = 100
  name = "tmp42"
  generate_data(n=n, name=name)
  df = spark.sql(f"select * from {name}")
  assert df.count() == n

# COMMAND ----------

def test_data_prediction():
  predicted = get_data_prediction()
  assert predicted == 42

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate the test suite
# MAGIC 
# MAGIC Some of Python packages for unit testign are relying on automatic tests discovery based on the file analysis, etc.  When working with Databricks notebooks this may not work, so we may need somehow generate the test suite that will be executed.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC For test classes we can either:
# MAGIC * build the test suite explicitly for every test class and each test case inside it, as in the `generate_test_class_suite` function
# MAGIC * automatically discover all test cases in the given class(-es) as in the `discover_test_cases` function
# MAGIC * discover all available test classes and their test cases completely automatically as in the `discover_test_classes` (in combination with `discover_test_cases`) by looking for classes with name ending with `Test`

# COMMAND ----------

def generate_test_class_suite():
  suite = unittest.TestSuite()
  suite.addTest(SimpleTest('test_data_generation'))
  suite.addTest(SimpleTest('test_data_prediction'))

  return suite

# COMMAND ----------

def discover_test_cases(*test_classes):
  suite = unittest.TestSuite()
  for test_class in test_classes:
    for test in unittest.defaultTestLoader.getTestCaseNames(test_class):
      suite.addTest(test_class(test))
      
  return suite

# COMMAND ----------

def discover_test_classes():
  classes = [obj for name, obj in globals().items()
    if name.endswith('Test') and obj.__module__ == '__main__' and isinstance(obj, type) and unittest.case.TestCase in set(obj.__bases__)]

  return discover_test_cases(*classes)

# COMMAND ----------

suite = generate_test_class_suite()
# or
suite = discover_test_cases(SimpleTest)
# or 
suite = discover_test_classes()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC For functions, test suite could be either built explicitly, like in the function `generate_function_suite`, or discovered by analyzing the global declarations as in the `discover_function_suite` function that includes into the test suite all functions with given prefix (`test_` in our example) - this could lead to some errors if you have functions with name starting with selected prefix that are defined in the current context:

# COMMAND ----------

def generate_function_suite(suite = None):
    if suite is None:
      suite = unittest.TestSuite()
    suite.addTest(unittest.FunctionTestCase(test_data_generation))
    suite.addTest(unittest.FunctionTestCase(test_data_prediction))
    return suite

# COMMAND ----------

def discover_function_suite(suite = None):
    if suite is None:
      suite = unittest.TestSuite()
    for name, obj in globals().items():
      if name.startswith('test_') and callable(obj) and obj.__module__ == '__main__':
        suite.addTest(unittest.FunctionTestCase(obj))
    
    return suite

# COMMAND ----------

suite = generate_function_suite()
# or
suite = discover_function_suite()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC and we can combine discovery of the both test functions & test classes:

# COMMAND ----------

def discover_test_classes_and_functions():
  return discover_function_suite(suite = discover_test_classes())

# COMMAND ----------

suite = discover_test_classes_and_functions()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Execute the test suite

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC When the test suite is generated, we can execute it and get testing results

# COMMAND ----------

# if we want to generate JUnit-compatible output, set to True
use_xml_runner = True

if use_xml_runner:
  import xmlrunner
  runner = xmlrunner.XMLTestRunner(output='/dbfs/tmp/test-reports')
else:
  runner = unittest.TextTestRunner()
results = runner.run(suite)

# COMMAND ----------

# MAGIC %sh ls -ls /dbfs/tmp/test-reports

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Use tests auto-discovery
# MAGIC  
# MAGIC For `unittest` library we may use tests auto-discovery, that will find tests implemented as classes.  The **main requirement for use with Databricks is to set `exit = False` in the list of arguments of `unittest.main` function.** It also makes sense to explicitly pass `argv` as single-element list, to avoid use of `sys.argv` that on Databricks contains parameters that were used to start Python subprocess. (see [documentation on `unittest.main`](https://docs.python.org/3.7/library/unittest.html#unittest.main))

# COMMAND ----------

test_runner = unittest.main(argv=[''], exit=False)
test_runner.result.printErrors()

if not test_runner.result.wasSuccessful():
  raise Exception(f"{len(test_runner.result.failures)} of {test_runner.result.testsRun} tests failed.")
