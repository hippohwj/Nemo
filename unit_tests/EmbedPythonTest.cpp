//
// Created by Zhihan Guo on 10/12/23.
//

#include "gtest/gtest.h"
#include <filesystem>

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "common/Common.h"

using namespace arboretum;

PyObject * PythonCreateConn(std::string &conn_str) {
  auto module = PyImport_Import(PyUnicode_DecodeFSDefault("AzureClientPython"));
  auto func = PyObject_GetAttrString(module, "create_conn");
  auto args = PyTuple_New(1);
  PyTuple_SetItem(args, 0, PyUnicode_FromString(conn_str.c_str()));
  return PyObject_CallObject(func, args);
}

long PythonRangeSearch(PyObject *object, const std::string& part,
                       const std::string& low_key,
                       const std::string& high_key,
                       std::string &data) {
  PyObject *pValue;
  pValue = PyObject_CallMethodObjArgs(object,
                                      PyUnicode_FromString("range_search"),
                                      PyUnicode_FromString("obj-0g-0"),
                                      PyUnicode_FromString(part.c_str()),
                                      PyUnicode_FromString(low_key.c_str()),
                                      PyUnicode_FromString(high_key.c_str()),
                                      NULL);
  if (pValue) {
    // PyList_Size
    auto return_sz = PyList_Size(pValue);
    for (Py_ssize_t i = 0; i < return_sz; i++) {
      auto obj = PyList_GetItem(pValue, i);
      auto key_obj = PyDict_GetItem(obj, PyUnicode_FromString("RowKey"));
      auto key = PyUnicode_AsUTF8(key_obj);
      // LOG_INFO("Row key of %zu-th item: %s\n", i, key);
      auto data_obj = PyDict_GetItem(obj, PyUnicode_FromString("Data"));
      data = std::string(PyBytes_AsString(data_obj), PyBytes_Size(data_obj));
      // LOG_INFO("Data of %zu-th item (size=%zu): %s\n", i, data.size(), data.c_str());
    }
    Py_DECREF(pValue);
    return return_sz;
  } else {
    PyErr_Print();
    LOG_ERROR("no pValue");
  }
  return -1;
}

static void ExecuteTest() {
  std::ifstream in(azure_config_fname);
  std::string azure_conn_str;
  azure_conn_str.resize(500);
  getline(in, azure_conn_str);
  auto module = PyImport_Import(PyUnicode_DecodeFSDefault("AzureClientPython"));
  auto dict = PyModule_GetDict(module);
  Py_DECREF(module);
  auto python_class = PyDict_GetItemString(dict, "PythonAzureClient");
  Py_DECREF(dict);
  auto object = PyObject_CallObject(python_class, nullptr);
  PyObject_CallMethodObjArgs(object,PyUnicode_FromString("create_conn"),
                             PyUnicode_FromString(azure_conn_str.c_str()), NULL);
  g_out_file.open("../output/microbench_latency.txt", std::ios_base::app);
  SearchKey start(10);
  long sz;
  for (uint64_t payload = 0; payload < 100; payload++) {
    auto low = start.ToString();
    start.SetValue(start.ToUInt64() + payload);
    auto starttime = GetSystemClock();
    for (int i = 0; i < 2; i++) {
      std::string data;
      sz = PythonRangeSearch(object, "0", low, start.ToString(), data);
    }
    auto latency = (GetSystemClock() - starttime) / 1000.0 / 2;
    EXPECT_EQ(sz, payload + 1);
    g_out_file << "scan," << payload + 1 << "," << latency << ",us" << std::endl;
    std::cout << "scan," << payload + 1 << "," << latency << ",us" << std::endl;
  }
}

TEST(EmbedPythonTest, InitTest) {
  // set python path
  auto cwd = std::filesystem::current_path().generic_string();
  std::ifstream pyin("configs/ifconfig_python.txt");
  std::stringstream ss;
  ss << cwd << "/python_deps";
  std::string line;
  while(getline(pyin, line)) {
    ss << ":" << line;
  }
  Py_SetProgramName(Py_DecodeLocale("python3", nullptr));
  Py_SetPath(Py_DecodeLocale(ss.str().c_str(), nullptr));
  Py_Initialize();
  std::thread thd(ExecuteTest);
  thd.join();
  Py_Finalize();
}


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::GTEST_FLAG(filter) = "EmbedPythonTest.*";
  return RUN_ALL_TESTS();
}

