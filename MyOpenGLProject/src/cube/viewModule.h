#ifndef __STEREOMODULE_H
#define __STEREOMODULE_H

#define IMAGE_WIDTH 1024
#define IMAGE_HEIGHT 768

#include <GL/glut.h>

typedef struct {
	GLdouble fieldOfView;
	GLdouble aspect;
	GLdouble nearPlane;
	GLdouble farPlane;
} PerspectiveData;

#endif
