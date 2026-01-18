#ifndef README_MD_SRC_REMOTE_AZUREPYTHONHELPER_H_
#define README_MD_SRC_REMOTE_AZUREPYTHONHELPER_H_



#include "common/Common.h"
#include "local/ITuple.h"

namespace arboretum {

static volatile int gil_init;

class AzurePythonHelper {

 public:
  // explicit AzurePythonHelper(const std::string &conn_str) {
  //   auto cwd = std::filesystem::current_path().generic_string();
  //   std::ifstream pyin("configs/ifconfig_python.txt");
  //   std::stringstream ss;
  //   ss << cwd << "/python_deps";
  //   std::string line;
  //   while (getline(pyin, line)) {
  //     ss << ":" << line;
  //   }
  //   Py_SetProgramName(Py_DecodeLocale("python3", nullptr));
  //   Py_SetPath(Py_DecodeLocale(ss.str().c_str(), nullptr));
  //   Py_Initialize();
  //   gil_init = 0;
  //   auto module = PyImport_Import(PyUnicode_DecodeFSDefault("AzureClientPython"));
  //   if (!module) {
  //     PyErr_Print();
  //     LOG_DEBUG("Failed to load 'AzureClientPython'\n");
  //   }
  //   auto dict = PyModule_GetDict(module);
  //   Py_DECREF(module);
  //   auto python_class = PyDict_GetItemString(dict, "PythonAzureClient");
  //   Py_DECREF(dict);
  //   object_ = PyObject_CallObject(python_class, nullptr);
  //   PyObject_CallMethodObjArgs(object_,
  //                              PyUnicode_FromString("create_conn"),
  //                              PyUnicode_FromString(conn_str.c_str()), NULL);
  // }
  explicit AzurePythonHelper(const std::string& conn_str) {
    auto cwd = std::filesystem::current_path().generic_string();
    std::ifstream pyin("configs/ifconfig_python.txt");
    std::stringstream ss;
    ss << cwd << "/python_deps";
    std::string line;
    while (getline(pyin, line)) {
      ss << ":" << line;
    }
  
    Py_SetProgramName(Py_DecodeLocale("python3", nullptr));
    Py_SetPath(Py_DecodeLocale(ss.str().c_str(), nullptr));
    Py_Initialize();
  
    gil_init = 0;
  
    // Import module
    PyObject* module_name = PyUnicode_DecodeFSDefault("AzureClientPython");
    PyObject* module = PyImport_Import(module_name);
    Py_DECREF(module_name);
    if (!module) {
      PyErr_Print();
      LOG_ERROR("Failed to import AzureClientPython");
      throw std::runtime_error("Failed to import AzureClientPython");
    }
  
    // Get PythonAzureClient class (NEW reference)
    PyObject* python_class = PyObject_GetAttrString(module, "PythonAzureClient");
    Py_DECREF(module);  // we don't need the module ref anymore
    if (!python_class) {
      PyErr_Print();
      LOG_ERROR("AzureClientPython.PythonAzureClient not found");
      throw std::runtime_error("PythonAzureClient not found");
    }
  
    // Instantiate client
    object_ = PyObject_CallObject(python_class, nullptr);
    Py_DECREF(python_class);
    if (!object_) {
      PyErr_Print();
      LOG_ERROR("Failed to construct PythonAzureClient");
      throw std::runtime_error("Failed to construct PythonAzureClient");
    }
  
    // Call create_conn(conn_str)
    PyObject* res = PyObject_CallMethod(object_, "create_conn", "s", conn_str.c_str());
    if (!res) {
      PyErr_Print();
      LOG_ERROR("PythonAzureClient.create_conn failed");
      throw std::runtime_error("create_conn failed");
    }
    Py_DECREF(res);
  }
  

  ~AzurePythonHelper() {
    Py_Finalize();
  }

  bool ExecPyRangeSearch(std::string& tbl_name, std::string& part, std::string& low, std::string& high,
                         char** tuples, size_t total_tuple_sz, size_t& res_cnt) {
    PyObject* pValue;
    if (!gil_init) {
      pyContextLock.lock();
      if (!gil_init) {
        gil_init = 1;
        PyEval_InitThreads();
        PyEval_SaveThread();
      }
      pyContextLock.unlock();
    }
    PyGILState_STATE gstate;
    gstate = PyGILState_Ensure();
    // LOG_DEBUG("request for %s - %s received", low.c_str(), high.c_str());
    int retry = 0;
    int max_retry = 10;
    while (retry < max_retry) {
      pValue = PyObject_CallMethodObjArgs(object_, PyUnicode_FromString("range_search"),
                                          PyUnicode_FromString(tbl_name.c_str()), PyUnicode_FromString(part.c_str()),
                                          PyUnicode_FromString(low.c_str()), PyUnicode_FromString(high.c_str()), NULL);
      if (pValue) {
        // PyList_Size
        auto return_sz = PyList_Size(pValue);
        // M_ASSERT(return_sz >= 0, "python range query return list with size < 0");
        res_cnt = (size_t)return_sz;
        // LOG_INFO("Returned %ld: %d results\n", return_sz, res_cnt);
        if (res_cnt > 0) {
          *tuples = (char*)MemoryAllocator::Alloc(total_tuple_sz * res_cnt);
          char** data_ptrs = NEW_SZ(char*, res_cnt); 
          auto next = sizeof(ITuple);
          for (int i = 0; i < res_cnt; i++) {
            data_ptrs[i] = &((*tuples)[next]);
            next += total_tuple_sz;
          }
          for (Py_ssize_t i = 0; i < return_sz; i++) {
            auto obj = PyList_GetItem(pValue, i);
            // auto key_obj = PyDict_GetItem(obj, PyUnicode_FromString("RowKey"));
            // auto key = PyUnicode_AsUTF8(key_obj);
            // LOG_INFO("Row key of %zu-th item: %s\n", i, key);
  
            auto data_obj = PyDict_GetItem(obj, PyUnicode_FromString("Data"));
            memcpy(data_ptrs[i], PyBytes_AsString(data_obj), PyBytes_Size(data_obj));
            // LOG_INFO("key = %s, copy data to %p with size %zu\n",
            //         key, data_ptrs[i], PyBytes_Size(data_obj));
          }
          DEALLOC(data_ptrs);
          Py_DECREF(pValue);
        } 
        break;


      } else {
        PyErr_Print();
        // M_ASSERT(false, "no pValue");
        LOG_DEBUG("request for %s - %s retry %d failed for no pValue", low.c_str(), high.c_str(), retry);
        // LOG_DEBUG("no pValue for retry %d", retry);
        retry += 1;
      }
    }
    if (pValue) {
      // LOG_DEBUG("request for %s - %s done", low.c_str(), high.c_str());
      PyGILState_Release(gstate);
      return true;
    } else {
      M_ASSERT(false, "no pValue after %d retries", retry);
      return false;
    }
  }



  // bool ExecPyPointQuery(const std::string& tbl_name,
  //                       std::string& part,
  //                       const std::string& row_key,
  //                       std::string& data_out) {
  //   PyObject* pValue = nullptr;
  //   if (!gil_init) {
  //     pyContextLock.lock();
  //     if (!gil_init) {
  //       gil_init = 1;
  //       PyEval_InitThreads();
  //       PyEval_SaveThread();
  //     }
  //     pyContextLock.unlock();
  //   }
  //   PyGILState_STATE gstate = PyGILState_Ensure();

  //   int retry = 0;
  //   const int max_retry = 10;

  //   while (retry < max_retry) {
  //     pValue = PyObject_CallMethodObjArgs(
  //         object_,
  //         PyUnicode_FromString("point_query"),
  //         PyUnicode_FromString(tbl_name.c_str()),
  //         PyUnicode_FromString(part.c_str()),
  //         PyUnicode_FromString(row_key.c_str()),
  //         NULL);

  //     if (pValue) {
  //       if (pValue == Py_None) {
  //         Py_DECREF(pValue);
  //         PyGILState_Release(gstate);
  //         return false; // not found
  //       }

  //       PyObject* data_obj = PyDict_GetItemString(pValue, "Data");
  //       if (data_obj && PyBytes_Check(data_obj)) {
  //         data_out.assign(PyBytes_AsString(data_obj),
  //                         PyBytes_Size(data_obj));
  //       } else if (data_obj && PyUnicode_Check(data_obj)) {
  //         data_out = PyUnicode_AsUTF8(data_obj);
  //       } else {
  //         data_out.clear();
  //       }

  //       Py_DECREF(pValue);
  //       PyGILState_Release(gstate);
  //       return true;
  //     } else {
  //       PyErr_Print();
  //       LOG_DEBUG("point_query retry %d failed for table %s partition %s key %s",
  //                 retry, tbl_name.c_str(), part.c_str(), row_key.c_str());
  //       retry++;
  //     }
  //   }

  //   PyGILState_Release(gstate);
  //   M_ASSERT(false, "ExecPyPointQuery failed after %d retries", retry);
  //   return false;
  // }

bool ExecPyPointQuery(const std::string& tbl_name,
                      std::string& part,
                      const std::string& row_key,
                      std::string& data_out)
{
  if (!object_) {
    LOG_ERROR("ExecPyPointQuery called with null Python client object");
    return false;
  }

  if (!gil_init) {
    std::lock_guard<std::mutex> lg(pyContextLock);
    if (!gil_init) {
      gil_init = 1;
      PyEval_InitThreads();
      PyEval_SaveThread();
    }
  }

  PyGILState_STATE gstate = PyGILState_Ensure();

  int retry = 0;
  const int max_retry = 10;
  PyObject* pValue = nullptr;

  while (retry < max_retry) {
    PyObject* method_name = PyUnicode_FromString("point_query");
    PyObject* py_tbl  = PyUnicode_FromString(tbl_name.c_str());
    PyObject* py_part = PyUnicode_FromString(part.c_str());
    PyObject* py_row  = PyUnicode_FromString(row_key.c_str());

    pValue = PyObject_CallMethodObjArgs(
        object_, method_name, py_tbl, py_part, py_row, NULL);

    Py_DECREF(method_name);
    Py_DECREF(py_tbl);
    Py_DECREF(py_part);
    Py_DECREF(py_row);

    if (pValue) {
      break;
    }

    PyErr_Print();
    LOG_DEBUG("point_query retry %d failed for table %s partition %s key %s",
              retry, tbl_name.c_str(), part.c_str(), row_key.c_str());
    retry++;
  }

  if (!pValue) {
    PyGILState_Release(gstate);
    M_ASSERT(false, "ExecPyPointQuery failed after %d retries", retry);
    return false;
  }

  // Handle None (not found)
  if (pValue == Py_None) {
    Py_DECREF(pValue);
    PyGILState_Release(gstate);
    return false;
  }

  // TableEntity behaves like a dict; use mapping protocol instead of dict-only API
  PyObject* data_obj = PyObject_GetItem(
      pValue, PyUnicode_FromString("Data"));  // gets new ref or raises

  if (!data_obj) {
    PyErr_Clear();  // no 'Data' key or error; treat as empty
    data_out.clear();
  } else if (PyBytes_Check(data_obj)) {
    data_out.assign(PyBytes_AsString(data_obj), PyBytes_Size(data_obj));
  } else if (PyUnicode_Check(data_obj)) {
    data_out = PyUnicode_AsUTF8(data_obj);
  } else {
    data_out.clear();
  }

  Py_XDECREF(data_obj);
  Py_DECREF(pValue);
  PyGILState_Release(gstate);
  return !data_out.empty();
}


  // bool ExecPyRangeSearch(std::string &tbl_name, std::string &part,
  //                        std::string &low, std::string &high,
  //                        char **data_ptrs, size_t& res_cnt) {
  //   PyObject *pValue;
  //   if (!gil_init) {
  //     pyContextLock.lock();
  //     if (!gil_init) {
  //       gil_init = 1;
  //       PyEval_InitThreads();
  //       PyEval_SaveThread();
  //     }
  //     pyContextLock.unlock();
  //   }
  //   PyGILState_STATE gstate;
  //   gstate = PyGILState_Ensure();
  //   // LOG_DEBUG("request for %s - %s received", low.c_str(), high.c_str());
  //   int retry = 0;
  //   int max_retry = 10;
  //   while (retry < max_retry) {
  //     pValue = PyObject_CallMethodObjArgs(object_, PyUnicode_FromString("range_search"),
  //                                         PyUnicode_FromString(tbl_name.c_str()), PyUnicode_FromString(part.c_str()),
  //                                         PyUnicode_FromString(low.c_str()), PyUnicode_FromString(high.c_str()), NULL);
  //     if (pValue) {
  //       // PyList_Size
  //       auto return_sz = PyList_Size(pValue);
  //       M_ASSERT(return_sz >=0, "python range query return list with size < 0");
  //       res_cnt = (size_t) return_sz;
  //       LOG_INFO("Returned %ld: %d results\n", return_sz, res_cnt);
  //       for (Py_ssize_t i = 0; i < return_sz; i++) {
  //         auto obj = PyList_GetItem(pValue, i);
  //         auto key_obj = PyDict_GetItem(obj, PyUnicode_FromString("RowKey"));
  //         auto key = PyUnicode_AsUTF8(key_obj);
  //         LOG_INFO("Row key of %zu-th item: %s\n", i, key);

  //         auto data_obj = PyDict_GetItem(obj, PyUnicode_FromString("Data"));
  //         memcpy(data_ptrs[i], PyBytes_AsString(data_obj), PyBytes_Size(data_obj));
  //         // LOG_INFO("key = %s, copy data to %p with size %zu\n",
  //         //         key, data_ptrs[i], PyBytes_Size(data_obj));
  //       }
  //       Py_DECREF(pValue);
  //       break;
  //     } else {
  //       PyErr_Print();
  //       // M_ASSERT(false, "no pValue");
  //       LOG_DEBUG("request for %s - %s retry %d failed for no pValue", low.c_str(), high.c_str(), retry);
  //       // LOG_DEBUG("no pValue for retry %d", retry);
  //       retry += 1;
  //     }
  //   }
  //   if (pValue) {
  //     // LOG_DEBUG("request for %s - %s done", low.c_str(), high.c_str());
  //     PyGILState_Release(gstate);
  //     return true;
  //   } else {
  //     M_ASSERT(false, "no pValue after %d retries", retry);
  //     return false;
  //   }

  // }

  std::mutex pyContextLock;
  PyObject *object_;
};
}

#endif //README_MD_SRC_REMOTE_AZUREPYTHONHELPER_H_
