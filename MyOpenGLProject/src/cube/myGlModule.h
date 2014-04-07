#ifndef __MYGLMODULE_H
#define __MYGLMODULE_H

#include <stack>
#include <math.h>
#include <GL/glut.h>

void setUsingMyGl(bool usingMyGl);
void multiplyMatrix(GLfloat* m, GLfloat* n, GLfloat* r);
void checkMatrixEmpty();
GLfloat* getMatrixStackTop();
void myGlPushMatrix();
void myGlPopMatrix();
void loadMatrix(GLfloat* newMatrix);
void myGlTranslatef(GLfloat x, GLfloat y, GLfloat z);
void myGlRotatef(GLfloat angle, GLfloat x, GLfloat y, GLfloat z);
void myGlScalef(GLfloat x, GLfloat y, GLfloat z);
void myGlLoadIdentity();
void normalizeVector(GLdouble* vector);
void computeCrossProductOfVectors(GLdouble* u, GLdouble* v, GLdouble* n);
void myGluLookAt(GLdouble eyeX, GLdouble eyeY, GLdouble eyeZ, GLdouble centerX, GLdouble centerY, GLdouble centerZ,
		GLdouble upX, GLdouble upY, GLdouble upZ);

#endif
