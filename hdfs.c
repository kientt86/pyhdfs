#include <Python.h>
#include <hdfs.h>
#include <arrayobject.h>

static hdfsFS* hdfsHandles = NULL;
static int num_hdfsHandles = 0;
PyObject *HDFSException;

/***
    Init connection to HDFS
    Input: host,port
    Ouput: connection handler  
***/    
static PyObject* py_connect(PyObject* self,PyObject* args)
{
    
    char *host;
    int port;
    if (!PyArg_ParseTuple(args, "si", &host, &port)){
        PyErr_SetString(HDFSException, "Parameter error");
        return NULL;
    }
    hdfsFS fs = hdfsConnect(host,port);
    if (fs == NULL)
    {
        PyErr_SetString(HDFSException, "Can not open a connection");
        return NULL;
    }    
    else
    {
        num_hdfsHandles++;
		hdfsHandles = realloc(hdfsHandles,num_hdfsHandles*sizeof(hdfsFS));
		hdfsHandles[num_hdfsHandles-1] = fs;
        //Return the current handle
        return Py_BuildValue("i",num_hdfsHandles-1);
    }
}


/***
    Read the content of a file on HDFS
    Input: Connection handler,hdfs_path
    Ouput: data
***/
static PyObject* py_read(PyObject* self,PyObject* args)
{
    int handle;
    char* path;
    PyArrayObject *result;


    if (!PyArg_ParseTuple(args,"is",&handle,&path)){
        PyErr_SetString(HDFSException, "Parameter error");
        return NULL;
    }
    //get the handle
    
	if (handle>=num_hdfsHandles){
		PyErr_SetString(HDFSException, "No such the connection");
		return NULL;
	}
	
	hdfsFS fs = hdfsHandles[handle];
    
    if(fs==NULL){
        PyErr_SetString(HDFSException, "Connection error");
        return NULL;
    }
    
    //Create buffer
    int bufferSize = 8192;
    char* buffer = malloc(sizeof(char)*bufferSize);
    char *data = malloc(0);;
    
    hdfsFile readFile = hdfsOpenFile(fs, path, O_RDONLY, bufferSize, 0, 0);
    
    tSize curSize = bufferSize;
	int len = 0;
    for (; curSize == bufferSize;) {
        curSize = hdfsRead(fs, readFile, (void*)buffer, bufferSize);
        len += curSize;
        data = realloc(data,len);
        memmove(data+len-curSize,buffer,curSize);
    }
    npy_intp dim[] = {len};
    result = (PyArrayObject *) PyArray_SimpleNew(1, dim, PyArray_CHAR);
    memcpy(result->data,data,len);
    return PyArray_Return(result);
}


/***
    Write data to a hdfs file
    Input: Connection handler,hdfs_path,data
    Output: Length of data write to file

***/
static PyObject* py_write(PyObject* self,PyObject* args)
{
    int handle,i,size;
    char* path;
    
    char *data;
    PyObject *aptr;
    PyArrayObject *array;
    
    if (!PyArg_ParseTuple(args,"isO",&handle,&path,&aptr)){
        PyErr_SetString(HDFSException, "Parameter error");
        return NULL;
    }
	
	if (handle>=num_hdfsHandles){
		PyErr_SetString(HDFSException, "No such the connection");
		return NULL;
	}
    
    array = (PyArrayObject *) PyArray_ContiguousFromObject(aptr, PyArray_CHAR, 1, 1);
    
    data = array->data;
    
    if(array->nd == 0)
        size = 1;
    else {
        size = 1;
        for(i=0;i<array->nd;i++)
            size = size * array->dimensions[i];
    }
    
    hdfsFS fs = hdfsHandles[handle];
    
    if (fs==NULL){
        PyErr_SetString(HDFSException, "Connection error");
        return NULL;        
    }
    hdfsFile writeFile = hdfsOpenFile(fs, path, O_WRONLY, size, 0, 0);
    hdfsWrite(fs, writeFile, (void*)data, size);
    hdfsCloseFile(fs, writeFile);
    return Py_BuildValue("i",size);
}

/***
    Delete a file on hdfs
    Input: Connection handler,hdfs_path
    Output:None
***/
static PyObject* py_delete(PyObject* self,PyObject* args)
{
    
    int handle;
    char* path; 
    if(!PyArg_ParseTuple(args,"is",&handle,&path)){
		PyErr_SetString(HDFSException, "Parameter error");
        return NULL;
	}
	
	if (handle>=num_hdfsHandles){
		PyErr_SetString(HDFSException, "No such the connection");
		return NULL;
	}
	
    hdfsFS fs = hdfsHandles[handle];
    if (fs==NULL){
        PyErr_SetString(HDFSException, "Connection error");
        return NULL;        
    }
	hdfsDelete(fs,path);
    return Py_None;
}

/***
    Check whether a folder of a file exist or not
    Input: Connection handler,hdfs_path
    Output: 1 or 0 (1: exist, 0: doesn't exist)
***/
static PyObject* py_exist(PyObject* self,PyObject* args)
{
    int handle;
    char *path;
    int exist;
    if(!PyArg_ParseTuple(args,"is",&handle,&path)){
        PyErr_SetString(HDFSException, "Parameter error");
        return NULL;
    }
	
	if (handle>=num_hdfsHandles){
		PyErr_SetString(HDFSException, "No such the connection");
		return NULL;
	}
	
    hdfsFS fs = hdfsHandles[handle];
    
    if (fs==NULL){
        PyErr_SetString(HDFSException, "Connection error");
        return NULL;        
    }
    
    if (hdfsExists(fs,path) == 0)
        exist = 1;
    else
        exist = 0;
    return Py_BuildValue("i",exist);
}

/***
    Disconnect to a connection
    Input: Connection handler
    Output: None
***/
static PyObject* py_disconnect(PyObject* self,PyObject* args)
{
    int handle;
    if(!PyArg_ParseTuple(args,"i",&handle)){
		PyErr_SetString(HDFSException, "Parameter error");
        return NULL;
	}
	
	if (handle>=num_hdfsHandles){
		PyErr_SetString(HDFSException, "No such the connection");
		return NULL;
	}
	
    hdfsFS fs = hdfsHandles[handle];
    if (fs!=NULL){
        hdfsDisconnect(fs);
        hdfsHandles[handle] = NULL;
        return Py_None;
    }else{
        PyErr_SetString(HDFSException, "Connection Closed");
        return NULL;
    }
}

static PyMethodDef HDFSMethods[] = {
    {"connect", py_connect, METH_VARARGS,"connect to a hdfs server"},
    {"disconnect", py_disconnect, METH_VARARGS,"disconnect to a hdfs server"},
	{"read", py_read, METH_VARARGS},
	{"delete", py_delete, METH_VARARGS},
	{"exist",py_exist,METH_VARARGS},
	{"write",py_write,METH_VARARGS},
	{NULL, NULL}
};

PyMODINIT_FUNC inithdfs(void)
{
    PyObject *hdfs;
    hdfs = Py_InitModule("hdfs", HDFSMethods);
    
    if(hdfs==NULL)
        return;
    
    import_array();
    
    HDFSException = PyErr_NewException("hdfs.error", NULL, NULL);
    Py_INCREF(HDFSException);
    PyModule_AddObject(hdfs, "error", HDFSException);
}
