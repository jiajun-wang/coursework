#ifndef __SHADERMODULE_H
#define __SHADERMODULE_H

#include <fstream>
#include <iostream>
#include <GL/glew.h>
#include <GL/glut.h>

unsigned long getFileLength(std::ifstream &file);
int loadShaderSource(const char* fileName, GLchar*& shaderSource);
void compileShader(GLuint &shader, GLint* shaderCompiled);
bool checkGlslRunnable();
int checkGlError(const char *file, int line);
void beginShader();
void endShader();
void initializeShader();
void toggleLight();

#endif
