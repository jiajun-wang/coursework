#include "sceneModule.h"
#include "myGlModule.h"

static GLfloat cubeColors[4][6][4] =
{
  {
    {0.8, 0.0, 0.0, 1.0},
    {0.8, 0.0, 0.2, 1.0},
    {0.8, 0.2, 0.0, 1.0},
    {0.8, 0.2, 0.2, 1.0},
    {0.6, 0.0, 0.0, 1.0},
    {0.6, 0.2, 0.2, 1.0}
  },
  {
    {0.0, 0.0, 0.8, 1.0},
    {0.0, 0.2, 0.8, 1.0},
    {0.2, 0.0, 0.8, 1.0},
    {0.2, 0.2, 0.8, 1.0},
    {0.0, 0.0, 0.6, 1.0},
    {0.2, 0.2, 0.6, 1.0}
  },
  {
    {0.0, 0.8, 0.0, 1.0},
    {0.2, 0.8, 0.0, 1.0},
    {0.0, 0.8, 0.2, 1.0},
    {0.2, 0.8, 0.2, 1.0},
    {0.0, 0.6, 0.0, 1.0},
    {0.2, 0.6, 0.2, 1.0}
  },
  {
    {0.8, 0.8, 0.9, 1.0},
    {0.9, 0.9, 0.8, 1.0},
    {0.8, 0.9, 0.8, 1.0},
    {0.9, 0.8, 0.9, 1.0},
    {0.9, 0.8, 0.8, 1.0},
    {0.8, 0.9, 0.9, 1.0}
  }
};

static GLfloat cubeNormals[6][3] =
{
  {-1.0, 0.0, 0.0},
  {1.0, 0.0, 0.0},
  {0.0, -1.0, 0.0},
  {0.0, 1.0, 0.0},
  {0.0, 0.0, -1.0},
  {0.0, 0.0, 1.0}
};

static GLfloat cubeVertexes[6][4][4] =
{
  {
    {-1.0, -1.0, -1.0, 1.0},
    {-1.0, -1.0, 1.0, 1.0},
    {-1.0, 1.0, 1.0, 1.0},
    {-1.0, 1.0, -1.0, 1.0}
  },
  {
    {1.0, 1.0, 1.0, 1.0},
    {1.0, -1.0, 1.0, 1.0},
    {1.0, -1.0, -1.0, 1.0},
    {1.0, 1.0, -1.0, 1.0}
  },
  {
    {-1.0, -1.0, -1.0, 1.0},
    {1.0, -1.0, -1.0, 1.0},
    {1.0, -1.0, 1.0, 1.0},
    {-1.0, -1.0, 1.0, 1.0}
  },
  {
    {1.0, 1.0, 1.0, 1.0},
    {1.0, 1.0, -1.0, 1.0},
    {-1.0, 1.0, -1.0, 1.0},
    {-1.0, 1.0, 1.0, 1.0}
  },
  {
    {-1.0, -1.0, -1.0, 1.0},
    {-1.0, 1.0, -1.0, 1.0},
    {1.0, 1.0, -1.0, 1.0},
    {1.0, -1.0, -1.0, 1.0}
  },
  {
    {1.0, 1.0, 1.0, 1.0},
    {-1.0, 1.0, 1.0, 1.0},
    {-1.0, -1.0, 1.0, 1.0},
    {1.0, -1.0, 1.0, 1.0}
  }
};

static GLfloat firstRotationAngle = 0.0;
static GLfloat secondRotationAngle = 0.0;
static GLfloat lightRotationAngle = 0.0;

void drawScene(bool usingMyGl) {
	setUsingMyGl(usingMyGl);

	myGlPushMatrix();

	glColor3f(1.0, 0.0, 0.0);
	glBegin(GL_LINES);
	glVertex3f(-20.0, 0.0, 0.0);
	glVertex3f(20.0, 0.0, 0.0);
	glEnd();

	glColor3f(0.0, 0.0, 1.0);
	glBegin(GL_LINES);
	glVertex3f(0.0, -20.0, 0.0);
	glVertex3f(0.0, 20.0, 0.0);
	glEnd();

	glColor3f(0.0, 1.0, 0.0);
	glBegin(GL_LINES);
	glVertex3f(0.0, 0.0, -20.0);
	glVertex3f(0.0, 0.0, 20.0);
	glEnd();

	myGlRotatef(firstRotationAngle, 0.0, 1.0, 0.0);

	drawCube(0);

	myGlTranslatef(0.0, 0.0, 6.0);
	myGlScalef(0.5, 0.5, 0.5);

	drawCube(1);

	myGlRotatef(secondRotationAngle, 0.0, 1.0, 0.0);
	myGlTranslatef(0.0, 0.0, 3.0);
	myGlScalef(0.5, 0.5, 0.5);

	drawCube(2);

	myGlPopMatrix();

	myGlPushMatrix();

	myGlRotatef(lightRotationAngle, -1.0, 0.0, 0.0);
	myGlScalef(0.5, 0.5, 0.5);
	myGlTranslatef(0.0, 0.0, 6.0);

	GLfloat light_position[] = {0.0, 0.0, 0.0, 1.0};
	glLightfv(GL_LIGHT0, GL_POSITION, light_position);

	drawCube(3);

	myGlPopMatrix();

	increaseRotationAngle(firstRotationAngle, 2.5);
	increaseRotationAngle(secondRotationAngle, 2.5);
	increaseRotationAngle(lightRotationAngle, 2.5);
}

void drawCube(int colorIndex) {
	for (int i = 0; i < 6; ++i) {
		glBegin(GL_POLYGON);
		glColor3fv(&cubeColors[colorIndex][i][0]);
		glNormal3fv(&cubeNormals[i][0]);
		glVertex4fv(&cubeVertexes[i][0][0]);
		glVertex4fv(&cubeVertexes[i][1][0]);
		glVertex4fv(&cubeVertexes[i][2][0]);
		glVertex4fv(&cubeVertexes[i][3][0]);
		glEnd();
	}
}

void increaseRotationAngle(GLfloat &rotationAngle, GLfloat increment) {
	rotationAngle += increment;
	if (rotationAngle > 360) {
		rotationAngle -= 360;
	}
}
