#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import shlex
import subprocess
import unittest

from pyspark import SparkConf, SparkFiles, SparkContext
from pyspark.testing.utils import PySparkTestCase, QuietTest

class CustomPythonPackageInstallationTests(PySparkTestCase):
    def _set_virtualenv_path(self):
        # Dynamically get virtualenv path
        command = "which virtualenv"
        process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        virtualenvBinPath = process.stdout.readline()
        self.sc._conf.set("spark.pyspark.virtualenv.bin.path", virtualenvBinPath)

    def test_install_pypi_package_without_enabling_virtualenv(self):
        self.sc._conf.set("spark.pyspark.virtualenv.enabled", "false")
        self.sc._conf.set("spark.pyspark.virtualenv.type", "native")
        self.sc._conf.set("spark.pyspark.python", "python")

        def call_install_pypi_package():
            self.sc.install_pypi_package("celery")
        self.assertRaises(RuntimeError, call_install_pypi_package)

    def test_install_pypi_package_for_invalid_input(self):
        self.sc._conf.set("spark.pyspark.virtualenv.enabled", "true")
        self.sc._conf.set("spark.pyspark.virtualenv.type", "native")
        self.sc._conf.set("spark.pyspark.python", "python")

        def call_install_pypi_package_with_non_string_package():
            self.sc.install_pypi_package(["celery"])

        def call_install_pypi_package_with_invalid_char_in_package():
            self.sc.install_pypi_package("invalid$%#")

        def call_install_pypi_package_with_non_string_repo():
            self.sc.install_pypi_package("celery", ["repo"])

        def call_install_pypi_package_with_invalid_char_in_repo():
            self.sc.install_pypi_package("celery", "invalid_repo")

        self.assertRaises(ValueError, call_install_pypi_package_with_non_string_package)
        self.assertRaises(ValueError, call_install_pypi_package_with_invalid_char_in_package)
        self.assertRaises(ValueError, call_install_pypi_package_with_non_string_repo)
        self.assertRaises(ValueError, call_install_pypi_package_with_invalid_char_in_repo)

    def test_install_pypi_package_locally(self):
        self.sc._conf.set("spark.pyspark.virtualenv.enabled", "true")
        self.sc._conf.set("spark.pyspark.virtualenv.type", "native")
        self._set_virtualenv_path()
        self.sc._conf.set("spark.pyspark.python", "python")

        def test_local():
            import celery
            return True
        self.assertRaises(ImportError, test_local)
        self.sc.install_pypi_package("celery")
        self.assertTrue(test_local)
        self.assertEqual("celery", self.sc._conf.get("spark.pyspark.virtualenv.packages"))
        self.sc.uninstall_package("celery")

    def test_install_pypi_package_locally_with_version_and_repo(self):
        self.sc._conf.set("spark.pyspark.virtualenv.enabled", "true")
        self.sc._conf.set("spark.pyspark.virtualenv.type", "native")
        self._set_virtualenv_path()
        self.sc._conf.set("spark.pyspark.python", "python")

        def test_local():
            import celery
            return True
        self.assertRaises(ImportError, test_local)
        self.sc.install_pypi_package("arrow==0.14.0", "https://pypi.org/simple")
        self.assertTrue(test_local)
        self.sc.uninstall_package("arrow")

    def test_install_pypi_package_remotely(self):
        self.sc._conf.set("spark.pyspark.virtualenv.enabled", "true")
        self.sc._conf.set("spark.pyspark.virtualenv.type", "native")
        self._set_virtualenv_path()
        self.sc._conf.set("spark.pyspark.python", "python")

        def test_remote(x):
            import celery
            return celery.__version__
        with QuietTest(self.sc):
            self.assertRaises(Exception, self.sc.parallelize(range(2)).map(test_remote).first)

        self.sc.install_pypi_package("celery")
        res = self.sc.range(2).map(test_remote).first
        self.assertIsNotNone(res)
        self.assertEqual("celery", self.sc._conf.get("spark.pyspark.virtualenv.packages"))
        self.sc.uninstall_package("celery")

    def test_install_pypi_package_remotely_with_version_and_repo(self):
        self.sc._conf.set("spark.pyspark.virtualenv.enabled", "true")
        self.sc._conf.set("spark.pyspark.virtualenv.type", "native")
        self._set_virtualenv_path()
        self.sc._conf.set("spark.pyspark.python", "python")

        def test_remote(x):
            import celery
            return celery.__version__
        with QuietTest(self.sc):
            self.assertRaises(Exception, self.sc.parallelize(range(2)).map(test_remote).first)

        self.sc.install_pypi_package("celery==4.2.2", "https://pypi.org/simple")
        res = self.sc.range(2).map(test_remote).first
        self.assertIsNotNone(res)
        self.sc.uninstall_package("celery")

    def test_uninstall_package_without_enabling_virtualenv(self):
        self.sc._conf.set("spark.pyspark.virtualenv.enabled", "false")
        self.sc._conf.set("spark.pyspark.virtualenv.type", "native")
        self.sc._conf.set("spark.pyspark.python", "python")

        def call_uninstall_package():
            self.sc.uninstall_package("celery")
        self.assertRaises(RuntimeError, call_uninstall_package)

    def test_uninstall_package_for_invalid_input(self):
        self.sc._conf.set("spark.pyspark.virtualenv.enabled", "true")
        self.sc._conf.set("spark.pyspark.virtualenv.type", "native")
        self.sc._conf.set("spark.pyspark.python", "python")

        def call_uninstall_package_with_non_string_input():
            self.sc.uninstall_package(["celery"])

        def call_uninstall_package_with_invalid_char_string():
            self.sc.uninstall_package("invalid%$#")

        self.assertRaises(ValueError, call_uninstall_package_with_non_string_input)
        self.assertRaises(ValueError, call_uninstall_package_with_invalid_char_string)

    def test_uninstall_package(self):
        self.sc._conf.set("spark.pyspark.virtualenv.enabled", "true")
        self.sc._conf.set("spark.pyspark.virtualenv.type", "native")
        self._set_virtualenv_path()
        self.sc._conf.set("spark.pyspark.python", "python")

        def test_uninstall():
            import celery
            return True
        self.sc.install_pypi_package("celery")
        self.assertTrue(test_uninstall())
        self.assertEqual("celery", self.sc._conf.get("spark.pyspark.virtualenv.packages"))
        self.sc.uninstall_package("celery")
        self.assertEqual("", self.sc._conf.get("spark.pyspark.virtualenv.packages"))

    def test_list_packages_without_enabling_virtualenv(self):
        self.sc._conf.set("spark.pyspark.virtualenv.enabled", "false")
        self.sc._conf.set("spark.pyspark.virtualenv.type", "native")
        self.sc._conf.set("spark.pyspark.python", "python")

        def call_list_packages():
            self.sc.list_packages()
        self.assertRaises(RuntimeError, call_list_packages)

    def test_list_packages(self):
        self.sc._conf.set("spark.pyspark.virtualenv.enabled", "true")
        self.sc._conf.set("spark.pyspark.virtualenv.type", "native")
        self._set_virtualenv_path()
        self.sc._conf.set("spark.pyspark.python", "python")
        try:
            self.sc.list_packages()
        except type:
            self.fail("test_list_packages raised: " + type)

    def test__get_package_from_descriptor(self):
        package = self.sc._get_package_from_descriptor("package3==2.0.0@http://some-repo/")
        self.assertEqual("package3", package)

    def test__add_package_descriptor(self):
        self.sc._add_package_descriptor("package1")
        self.assertEqual("package1", self.sc._conf.get("spark.pyspark.virtualenv.packages"))
        self.sc._add_package_descriptor("package2==1.0.0")
        self.assertEqual("package1,package2==1.0.0", self.sc._conf.get("spark.pyspark.virtualenv.packages"))
        self.sc._add_package_descriptor("package3==2.0.0", "http://some-repo/")
        self.assertEqual("package1,package2==1.0.0,package3==2.0.0@http://some-repo/",
            self.sc._conf.get("spark.pyspark.virtualenv.packages"))

    def test__remove_package_descriptor(self):
        self.sc._conf.set("spark.pyspark.virtualenv.packages",
            "package1,package2==1.0.0,package3==2.0.0@http://some-repo/")
        self.sc._remove_package_descriptor("package1")
        self.assertEqual("package2==1.0.0,package3==2.0.0@http://some-repo/",
            self.sc._conf.get("spark.pyspark.virtualenv.packages"))
        self.sc._remove_package_descriptor("package2")
        self.assertEqual("package3==2.0.0@http://some-repo/",
            self.sc._conf.get("spark.pyspark.virtualenv.packages"))
        self.sc._remove_package_descriptor("package3")
        self.assertEqual("", self.sc._conf.get("spark.pyspark.virtualenv.packages"))

    def test_install_pypi_package_for_duplicate_package(self):
        self.sc._conf.set("spark.pyspark.virtualenv.enabled", "true")
        self.sc._conf.set("spark.pyspark.virtualenv.type", "native")
        self._set_virtualenv_path()
        self.sc._conf.set("spark.pyspark.python", "python")

        def test_duplicate():
            self.sc.install_pypi_package("arrow==0.12.1")
            self.sc.install_pypi_package("arrow")

        self.assertRaises(ValueError, test_duplicate)

if __name__ == "__main__":
    from pyspark.tests.test_custom_python_package import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
