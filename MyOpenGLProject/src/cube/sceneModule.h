#ifndef __SCENEMODULE_H
#define __SCENEMODULE_H

#include <GL/glut.h>

void drawScene(bool usingMyGl);
void drawCube(int colorIndex);
void increaseRotationAngle(GLfloat &rotationAngle, GLfloat increment);

#endif
