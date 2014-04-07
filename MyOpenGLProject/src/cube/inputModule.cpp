#include "shaderModule.h"
#include "inputModule.h"
#include "myGlModule.h"

static int motionMode;
static int startX;
static int startY;
static GLdouble xDistance = 10.0;
static GLdouble yDistance = 10.0;
static GLdouble zDistance = 10.0;
static GLdouble xUp = calculateXUp(xDistance, yDistance, zDistance);
static GLdouble yUp = calculateYUp(xDistance, yDistance, zDistance);
static GLdouble zUp = calculateZUp(xDistance, yDistance, zDistance);

void readKeyboard(unsigned char key, int x, int y) {
	switch (key) {
		case 'r':
		case 'R': {
			/* reset initial view parameters */
			xDistance = 10.0;
			yDistance = 10.0;
			zDistance = 10.0;
			xUp = calculateXUp(xDistance, yDistance, zDistance);
			yUp = calculateYUp(xDistance, yDistance, zDistance);
			zUp = calculateZUp(xDistance, yDistance, zDistance);

			break;
		}

		case 'l':
		case 'L': {
			toggleLight();

			break;
		}
	}

	setUserView();
}

void mouseButtHandler(int button, int state, int x, int y) {
	motionMode = 0;

	switch (button) {
		case GLUT_LEFT_BUTTON: {
			if (state == GLUT_DOWN) {
				/* Rotate object */
				motionMode = 1;
				startX = x;
				startY = y;
			}

			break;
		}

		case GLUT_RIGHT_BUTTON: {
			if (state == GLUT_DOWN) {
				/* Zoom */
				motionMode = 2;
				startX = x;
				startY = y;
			}

			break;
		}
	}

	setUserView();
}

void mouseMoveHandler(int x, int y) {
	switch (motionMode) {
		case 1: {
			/* Calculate the rotations */
			double xMove = (x - startX) / 100.0;
			double yMove = (y - startY) / 100.0;
			GLdouble xzDistance = sqrt(pow(xDistance, 2) + pow(zDistance, 2));

			if (pow(xMove, 2) >= pow(yMove, 2)) {
				double xAngle = 0.0;

				if (zDistance >= 0) {
					xAngle = acos(xDistance / xzDistance);
				} else {
					xAngle = 2 * M_PI - acos(xDistance / xzDistance);
				}

				double newXAngle = xAngle + xMove;
				xDistance = xzDistance * cos(newXAngle);
				zDistance = xzDistance * sin(newXAngle);
				xUp = calculateXUp(xDistance, yDistance, zDistance);
				zUp = calculateZUp(xDistance, yDistance, zDistance);

			} else {
				GLdouble distance = sqrt(pow(xDistance, 2) + pow(yDistance, 2) + pow(zDistance, 2));
				double yAngle = asin(yDistance / distance);
				double newYAngle = yAngle + yMove;

				if (newYAngle > -M_PI_2 && newYAngle < M_PI_2) {
					GLdouble ratio = distance * cos(newYAngle) / xzDistance;
					xDistance *= ratio;
					zDistance *= ratio;
					yDistance = distance * sin(newYAngle);
					xUp = calculateXUp(xDistance, yDistance, zDistance);
					zUp = calculateZUp(xDistance, yDistance, zDistance);
					yUp = calculateYUp(xDistance, yDistance, zDistance);
				}
			}

			startX = x;
			startY = y;

			break;
		}

		case 2: {
			GLdouble distance = sqrt(pow(xDistance, 2) + pow(yDistance, 2) + pow(zDistance, 2));
			GLdouble newDistance = distance - (y - startY) / 10.0;

			if (newDistance > 2 && distance > 0) {
				GLdouble ratio = newDistance / distance;
				xDistance *= ratio;
				yDistance *= ratio;
				zDistance *= ratio;
			}

			startX = x;
			startY = y;

			break;
		}
	}

	setUserView();
}

GLdouble calculateXUp(GLdouble x, GLdouble y, GLdouble z) {
	if (y == 0) {
		return 0;
	} else if (y > 0) {
		return -x;
	} else {
		return x;
	}
}

GLdouble calculateZUp(GLdouble x, GLdouble y, GLdouble z) {
	if (y == 0) {
		return 0;
	} else if (y > 0) {
		return -z;
	} else {
		return z;
	}
}

GLdouble calculateYUp(GLdouble x, GLdouble y, GLdouble z) {
	if (y == 0) {
		return 1;
	} else if (y > 0) {
		return (pow(x, 2) + pow(z, 2)) / y;
	} else {
		return -(pow(x, 2) + pow(z, 2)) / y;
	}
}

void setUserView(bool usingMyGl) {
	setUsingMyGl(usingMyGl);

	setUserView();
}

void setUserView() {
	myGlLoadIdentity();
	myGluLookAt(xDistance, yDistance, zDistance, 0.0, 0.0, 0.0, xUp, yUp, zUp);
}
