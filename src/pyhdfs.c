/*
    Author: KienTrinh
    Email: kientt86@gmail.com
    Skype:kientt86
*/

#include <Python.h>
#include <hdfs.h>
#include <arrayobject.h>
#include "structmember.h"

staticforward PyTypeObject py_hdfsFS_Type;
staticforward PyTypeObject py_hdfsFile_Type;
PyObject *HDFSException;

typedef struct {
    PyObject_HEAD
    hdfsFS fs;
    PyObject *host;
    PyObject *port;
} py_hdfsFS;

typedef struct {
    PyObject_HEAD
    hdfsFS fs;    
    hdfsFile file;
    PyObject *path;
    int mode;
} py_hdfsFile;

static void hdfs_dealloc(PyObject* self)
{
    PyObject_Del(self);
}

/**
 * Open a file for read or write
 * @param path: Path to file
 * @param mode: Mode to open file
 * @return Returns py_hdfsFile object
 */

static PyObject* hdfsFS_open(py_hdfsFS* self,PyObject* args)
{
    char* path;
    char* mode;
    int bufferSize = 0;
    int replication = 0;
    int blocksize = 0;
    int flag;
    //Check parse params
    if(!(PyArg_ParseTuple(args,"ss|iii",&path,&mode,&bufferSize,&replication,&blocksize)))
    {
        PyErr_SetString(HDFSException, "Parameter error");
        return NULL;
    }
    if (self->fs==NULL)
    {
        PyErr_SetString(HDFSException, "Connection closed");
        return NULL;
    }
    //checkMode
    if (!strcmp(mode, "r")){
        flag = O_RDONLY;
    }else if (!strcmp(mode, "w")){
        flag = O_WRONLY;
    }else if (!strcmp(mode, "a")){
        PyErr_SetString(HDFSException, "Append mode is not supported in HDFS");
        return NULL;
    }else{
        PyErr_SetString(HDFSException, "Wrong mode when opening a file");
        return NULL;
    }
    hdfsFile file = hdfsOpenFile(self->fs,path,flag,bufferSize,replication,blocksize);
    if (file==NULL){
        PyErr_SetString(HDFSException, "Can not open file");
        return NULL;
    }else{
        py_hdfsFile *py_file  = PyObject_New(py_hdfsFile, &py_hdfsFile_Type);
        py_file->fs = self->fs;
        py_file->file = file;
        py_file->path = Py_BuildValue("s",path);
        py_file->mode = flag;
        return (PyObject*)py_file;
    }   
}

/**
 * Read an opened file
 * @return Array of bytes
 */
static PyObject* py_read(py_hdfsFile* self,PyObject* args)
{
    if(self->fs==NULL || self->file==NULL){
        PyErr_SetString(HDFSException, "File pointer error or connection closed");
        return NULL;
    }
    
    if (self->mode!= O_RDONLY){
        PyErr_SetString(HDFSException, "File just open for reading, Invalid operation");
        return NULL;
    }
    
    //Read the file
    PyArrayObject *result; 
    int bufferSize = 8192;
    char* buffer = malloc(sizeof(char)*bufferSize);
    char *data = malloc(0);
    tSize curSize = bufferSize;
	int len = 0;
    for (; curSize == bufferSize;) {
        curSize = hdfsRead(self->fs, self->file, (void*)buffer, bufferSize);
        len += curSize;
        data = realloc(data,len);
        memmove(data+len-curSize,buffer,curSize);
    }
    npy_intp dim[] = {len};
    result = (PyArrayObject *) PyArray_SimpleNew(1, dim, PyArray_CHAR);
    memcpy(result->data,data,len);
    free(data);
    free(buffer);
    return PyArray_Return(result);
}

/**
 * Write data to an opened file
 * @param data: Data to write to file
 * @return Size of data write to file
 */
static PyObject* py_write(py_hdfsFile* self,PyObject* args)
{
    if(self->fs==NULL || self->file==NULL){
        PyErr_SetString(HDFSException, "File pointer error or connection closed");
        return NULL;
    }
    if (self->mode!= O_WRONLY){
        PyErr_SetString(HDFSException, "File just open for reading, Invalid operation");
        return NULL;
    }
    
    PyObject *aptr;
    char *data;
    int i,size;
    PyArrayObject *array;
    if (!PyArg_ParseTuple(args,"O",&aptr)){
        PyErr_SetString(HDFSException, "Parameter error");
        return NULL;
    }
    array = (PyArrayObject *) PyArray_ContiguousFromObject(aptr, PyArray_CHAR, 1, 1);
    data = array->data;
    //Get size of data
    if(array->nd == 0)
        size = 1;
    else {
        size = 1;
        for(i=0;i<array->nd;i++)
            size = size * array->dimensions[i];
    }
    hdfsWrite(self->fs, self->file, (void*)data, size);
    free(data);
    return Py_BuildValue("i",size);
}

/**
 * Close a file after read or write
 * @return None
 */
static PyObject* py_close(py_hdfsFile* self,PyObject* args)
{
    if(self->fs==NULL || self->file==NULL){
        PyErr_SetString(HDFSException, "File pointer error or connection closed");
        return NULL;
    }
    hdfsCloseFile(self->fs,self->file);
    self->file = NULL;
    return Py_None;
}

/**
 * Init a connection to HDFS
 * @params host: hadoop master host
 * @parans port: hadoop master port
 * @return py_hdfsFS Object
 */
static int pyhfsFS_init(py_hdfsFS *self, PyObject *args, PyObject *kwds){
    PyObject *tmp;
    char *host;
    int port;
    static char *kwlist[] = {"host", "port",NULL};
    if (! PyArg_ParseTupleAndKeywords(args, kwds, "si", kwlist,&host,&port))
        return -1;
    hdfsFS fs = hdfsConnect(host,port);
    tmp = self->fs;
    self->fs = fs;
    
    tmp = self->host;
    self->host = Py_BuildValue("s",host);
    Py_XDECREF(tmp);
    
    tmp = self->port;
    self->port = Py_BuildValue("i",port);
    Py_XDECREF(tmp);
    return 0;
}

/**
 * Close the connection to HDFS
 * @return None
 */
static PyObject* hdfsFS_disconnect(py_hdfsFS* self,PyObject* args)
{
    if (self->fs==NULL){
        PyErr_SetString(HDFSException, "Connection Closed");
        return NULL;
    }
    hdfsDisconnect(self->fs);
    self->fs = NULL;
    return Py_None;
}

/**
 * Close the connection to HDFS
 * @return None
 */
static PyObject* hdfsFS_delete(py_hdfsFS* self,PyObject* args)
{
    if (self->fs==NULL){
        PyErr_SetString(HDFSException, "Connection Closed");
        return NULL;
    }
    char *path;
    if (!PyArg_ParseTuple(args,"s",&path)){
        PyErr_SetString(HDFSException, "Parameter error");
        return NULL;
    }
    if (hdfsExists(self->fs,path)){
        PyErr_SetString(HDFSException, "File not found");
        return NULL;
    }
    return Py_BuildValue("i",hdfsDelete(self->fs,path));
}


/**
 * Rename a file or directory
 * @params old_path: old hdfs path
 * @params new_path: new hdfs path   
 * @return : 0 if success, -1 if error
 */
static PyObject* hdfsFS_rename(py_hdfsFS* self,PyObject* args)
{
    if (self->fs==NULL){
        PyErr_SetString(HDFSException, "Connection Closed");
        return NULL;
    }
    char *old_path;
    char *new_path;
    if (!PyArg_ParseTuple(args,"ss",&old_path,&new_path)){
        PyErr_SetString(HDFSException, "Parameter error");
        return NULL;
    }
    return Py_BuildValue("i",hdfsRename(self->fs,old_path,new_path));
}

/**
 * Check a file or directory exist or not
 * @params path: new hdfs path   
 * @return : 1 if exist, 0 if not exist
 */
static PyObject* hdfsFS_exist(py_hdfsFS* self,PyObject* args)
{
    if (self->fs==NULL){
        PyErr_SetString(HDFSException, "Connection Closed");
        return NULL;
    }
    char *path;
    if (!PyArg_ParseTuple(args,"s",&path)){
        PyErr_SetString(HDFSException, "Parameter error");
        return NULL;
    }
    if (hdfsExists(self->fs,path)==0)
        return Py_BuildValue("i",1);
    else
        return Py_BuildValue("i",0);
        
}

/**
 * Create a directory
 * @params path: new hdfs directory
 * @return : 0 if success, -1 if error
 */
static PyObject* hdfsFS_mkdir(py_hdfsFS* self,PyObject* args)
{
    if (self->fs==NULL){
        PyErr_SetString(HDFSException, "Connection Closed");
        return NULL;
    }
    char *path;
    if (!PyArg_ParseTuple(args,"s",&path)){
        PyErr_SetString(HDFSException, "Parameter error");
        return NULL;
    }    
    return Py_BuildValue("i",hdfsCreateDirectory(self->fs,path));
}


static PyMemberDef pyhdfsFS_members[] = {
    {"host", T_OBJECT_EX, offsetof(py_hdfsFS, host), 0,"HDFS host"},
    {"port", T_OBJECT_EX, offsetof(py_hdfsFS, port), 0,"HDFS port"}
};



static PyMethodDef pyhdfs_methods[] = {
    {NULL, NULL, 0, NULL}
};
static PyMethodDef pyhdfsFS_methods[] = {
    {"disconnect", (PyCFunction)hdfsFS_disconnect, METH_VARARGS,"Disconnect"},
    {"exist", (PyCFunction)hdfsFS_exist, METH_VARARGS,"Check a file or directory exist or not"},
    {"delete", (PyCFunction)hdfsFS_delete, METH_VARARGS,"Delete a file"},
    {"rename", (PyCFunction)hdfsFS_rename, METH_VARARGS,"Rename a file"},
    {"mkdir", (PyCFunction)hdfsFS_mkdir, METH_VARARGS,"Create a directory"},
    {"open", (PyCFunction)hdfsFS_open, METH_VARARGS,"Open A file"},
    {NULL, NULL, 0, NULL}
};

static PyMemberDef pyhdfsFile_members[] = {
    {"path", T_OBJECT_EX, offsetof(py_hdfsFile, path), 0,"Path"},
    {"mode", T_OBJECT_EX, offsetof(py_hdfsFile, mode), 0,"Mode"}
};

static PyMethodDef pyhdfsFile_methods[] = {
    {"read", (PyCFunction)py_read, METH_VARARGS,"Open file"},
    {"write", (PyCFunction)py_write, METH_VARARGS,"Open file"},
    {"close", (PyCFunction)py_close, METH_VARARGS,"Close file"},
    {NULL, NULL, 0, NULL}

};
static PyTypeObject py_hdfsFS_Type = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "pyhdfs.hdfsFile",         /*tp_name*/
    sizeof(py_hdfsFS),       /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    hdfs_dealloc,              /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    "hdfsFS objects",        /* tp_doc */
    0,		                   /* tp_traverse */
    0,		                   /* tp_clear */
    0,		                   /* tp_richcompare */
    0,		                   /* tp_weaklistoffset */
    0,		                   /* tp_iter */
    0,		                   /* tp_iternext */
    pyhdfsFS_methods,         /* tp_methods */
    pyhdfsFS_members,         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)pyhfsFS_init,     /* tp_init */
    0,     /* tp_alloc */
    0                          /* tp_new */
};

static PyTypeObject py_hdfsFile_Type = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "pyhdfs.hdfsFile",         /*tp_name*/
    sizeof(py_hdfsFile),       /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    hdfs_dealloc,              /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    "hdfsFile objects",        /* tp_doc */
    0,		                   /* tp_traverse */
    0,		                   /* tp_clear */
    0,		                   /* tp_richcompare */
    0,		                   /* tp_weaklistoffset */
    0,		                   /* tp_iter */
    0,		                   /* tp_iternext */
    pyhdfsFile_methods,        /* tp_methods */
    pyhdfsFile_members,        /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    0,                         /* tp_init */
    0,                         /* tp_alloc */
    0                          /* tp_new */
};

#ifndef PyMODINIT_FUNC	/* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif
PyMODINIT_FUNC
initpyhdfs(void) 
{
    import_array();
    PyObject* m;
    py_hdfsFS_Type.tp_new = PyType_GenericNew;
    py_hdfsFile_Type.tp_new = PyType_GenericNew;
    
    if (PyType_Ready(&py_hdfsFS_Type) < 0 || PyType_Ready(&py_hdfsFile_Type))
        return;

    m = Py_InitModule3("pyhdfs", pyhdfs_methods,
                       "Example module that creates an extension type.");

    Py_INCREF(&py_hdfsFS_Type);
    PyModule_AddObject(m, "HDFS", (PyObject *)&py_hdfsFS_Type);
    
    HDFSException = PyErr_NewException("HDFS.ERROR", NULL, NULL);
    Py_INCREF(HDFSException);
    PyModule_AddObject(m, "error", HDFSException);
}
