#ifndef __INPUTMODULE_H
#define __INPUTMODULE_H

#include <math.h>
#include <GL/glut.h>

extern int screenWidth;
extern int screenHeight;

void readKeyboard(unsigned char key, int x, int y);
void mouseButtHandler(int button, int state, int x, int y);
void mouseMoveHandler(int x, int y);
GLdouble calculateXUp(GLdouble x, GLdouble y, GLdouble z);
GLdouble calculateZUp(GLdouble x, GLdouble y, GLdouble z);
GLdouble calculateYUp(GLdouble x, GLdouble y, GLdouble z);
void setUserView(bool usingMyGl);
void setUserView();

#endif
