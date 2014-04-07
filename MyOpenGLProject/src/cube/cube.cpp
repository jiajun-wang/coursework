#include "shaderModule.h"
#include "sceneModule.h"
#include "viewModule.h"
#include "inputModule.h"

int window;
int screenWidth;
int screenHeight;

PerspectiveData perspectiveData;

bool usingMyGl = true;
bool usingTeapot = false;

void cleanUp(int sig) {
	// insert cleanup code here (i.e. deleting structures or so)
	exit(0);
}

// OpenGL Display function
void display(void) {
	glutSetWindow(window);
	glClear(GL_COLOR_BUFFER_BIT | GL_DEPTH_BUFFER_BIT);
	glColor3f(1.0, 1.0, 1.0);

	beginShader();

	/* Draw the scene into the back buffer */
	if (!usingTeapot) {
		drawScene(usingMyGl);

	} else {
		glutWireTeapot(5.0);
	}

	//endShader();

	/* Swap the front buffer with the back buffer - assumes double buffering */
	glutSwapBuffers();
}

void idle(void) {
	glutPostRedisplay();
}

// Initialize display settings
void initializeDisplay() {
	glClearColor(0.0, 0.0, 0.0, 1.0);
	glClearIndex(0);
	glClearDepth(1);
	glShadeModel(GL_SMOOTH);
	glEnable(GL_DEPTH_TEST);
	glDisable(GL_CULL_FACE);

	/* Perspective projection parameters */
	perspectiveData.fieldOfView = 45.0;
	perspectiveData.aspect = (GLdouble) IMAGE_WIDTH / IMAGE_HEIGHT;
	perspectiveData.nearPlane = 0.1;
	perspectiveData.farPlane = 50.0;

	/* setup context */
	glMatrixMode(GL_PROJECTION);
	glLoadIdentity();
	gluPerspective(perspectiveData.fieldOfView, perspectiveData.aspect, perspectiveData.nearPlane,
			perspectiveData.farPlane);

	glMatrixMode(GL_MODELVIEW);

	/* Set up where the projection */
	setUserView(usingMyGl);

	GLfloat light_position[] = {0.0, 3.0, 3.0, 1.0};
	GLfloat lmodel_ambient[] = {0.2, 0.2, 0.2, 1.0};

	glLightfv(GL_LIGHT0, GL_POSITION, light_position);
	glLightModelfv(GL_LIGHT_MODEL_AMBIENT, lmodel_ambient);

	glEnable(GL_LIGHTING);
	glEnable(GL_LIGHT0);
	glEnable(GL_RESCALE_NORMAL);

	initializeShader();
}

// Main function
int main(int argc, char **argv) {
	glutInit(&argc, argv);
	glutInitDisplayMode(GLUT_DOUBLE | GLUT_RGB | GLUT_DEPTH | GLUT_MULTISAMPLE);

	screenWidth = glutGet(GLUT_SCREEN_WIDTH);
	screenHeight = glutGet(GLUT_SCREEN_HEIGHT);
	glutInitWindowPosition((screenWidth - IMAGE_WIDTH) / 2, (screenHeight - IMAGE_HEIGHT) / 2);
	glutInitWindowSize(IMAGE_WIDTH, IMAGE_HEIGHT);

	window = glutCreateWindow(argv[0]);

	/* Register the appropriate call back functions with GLUT */
	glutDisplayFunc(display);
	glutKeyboardFunc(readKeyboard);
	glutMouseFunc(mouseButtHandler);
	glutMotionFunc(mouseMoveHandler);
	glutPassiveMotionFunc(mouseMoveHandler);
	glutIdleFunc(idle);

	initializeDisplay();

	/* This function doesn't return - put all clean up code in the cleanup function */
	glutMainLoop();

	/* ANSI C requires main to return an integer. */
	return (0);
}
