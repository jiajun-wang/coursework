#include "myGlModule.h"

static bool callingMyGl = false;
static std::stack<GLfloat *> myGlMatrixStack;

void setUsingMyGl(bool usingMyGl) {
	callingMyGl = usingMyGl;
}

void multiplyMatrix(GLfloat* m, GLfloat* n, GLfloat* r) {
	r[0] = m[0] * n[0] + m[4] * n[1] + m[8] * n[2] + m[12] * n[3];
	r[1] = m[1] * n[0] + m[5] * n[1] + m[9] * n[2] + m[13] * n[3];
	r[2] = m[2] * n[0] + m[6] * n[1] + m[10] * n[2] + m[14] * n[3];
	r[3] = m[3] * n[0] + m[7] * n[1] + m[11] * n[2] + m[15] * n[3];

	r[4] = m[0] * n[4] + m[4] * n[5] + m[8] * n[6] + m[12] * n[7];
	r[5] = m[1] * n[4] + m[5] * n[5] + m[9] * n[6] + m[13] * n[7];
	r[6] = m[2] * n[4] + m[6] * n[5] + m[10] * n[6] + m[14] * n[7];
	r[7] = m[3] * n[4] + m[7] * n[5] + m[11] * n[6] + m[15] * n[7];

	r[8] = m[0] * n[8] + m[4] * n[9] + m[8] * n[10] + m[12] * n[11];
	r[9] = m[1] * n[8] + m[5] * n[9] + m[9] * n[10] + m[13] * n[11];
	r[10] = m[2] * n[8] + m[6] * n[9] + m[10] * n[10] + m[14] * n[11];
	r[11] = m[3] * n[8] + m[7] * n[9] + m[11] * n[10] + m[15] * n[11];

	r[12] = m[0] * n[12] + m[4] * n[13] + m[8] * n[14] + m[12] * n[15];
	r[13] = m[1] * n[12] + m[5] * n[13] + m[9] * n[14] + m[13] * n[15];
	r[14] = m[2] * n[12] + m[6] * n[13] + m[10] * n[14] + m[14] * n[15];
	r[15] = m[3] * n[12] + m[7] * n[13] + m[11] * n[14] + m[15] * n[15];
}

void checkMatrixEmpty() {
	if (myGlMatrixStack.empty()) {
		GLfloat* identityMatrix = new GLfloat[16];
		identityMatrix[0] = 1.0;
		identityMatrix[1] = 0.0;
		identityMatrix[2] = 0.0;
		identityMatrix[3] = 0.0;

		identityMatrix[4] = 0.0;
		identityMatrix[5] = 1.0;
		identityMatrix[6] = 0.0;
		identityMatrix[7] = 0.0;

		identityMatrix[8] = 0.0;
		identityMatrix[9] = 0.0;
		identityMatrix[10] = 1.0;
		identityMatrix[11] = 0.0;

		identityMatrix[12] = 0.0;
		identityMatrix[13] = 0.0;
		identityMatrix[14] = 0.0;
		identityMatrix[15] = 1.0;

		myGlMatrixStack.push(identityMatrix);
	}
}

GLfloat* getMatrixStackTop() {
	checkMatrixEmpty();

	return myGlMatrixStack.top();
}

void myGlPushMatrix() {
	if (!callingMyGl) {
		glPushMatrix();

	} else {
		GLfloat* topMatrix = getMatrixStackTop();
		GLfloat* newTopMatrix = new GLfloat[16];

		for (int i = 0; i < 16; i++) {
			newTopMatrix[i] = topMatrix[i];
		}

		myGlMatrixStack.push(newTopMatrix);

		glLoadMatrixf(newTopMatrix);
	}
}

void myGlPopMatrix() {
	if (!callingMyGl) {
		glPopMatrix();

	} else {
		if (!myGlMatrixStack.empty()) {
			GLfloat* topMatrix = getMatrixStackTop();
			myGlMatrixStack.pop();

			delete[] topMatrix;
		}

		glLoadMatrixf(getMatrixStackTop());
	}
}

void loadMatrix(GLfloat* newMatrix) {
	GLfloat* topMatrix = getMatrixStackTop();
	GLfloat* newTopMatrix = new GLfloat[16];
	multiplyMatrix(topMatrix, newMatrix, newTopMatrix);

	myGlMatrixStack.pop();
	myGlMatrixStack.push(newTopMatrix);

	glLoadMatrixf(newTopMatrix);

	delete[] topMatrix;
}

void myGlTranslatef(GLfloat x, GLfloat y, GLfloat z) {
	if (!callingMyGl) {
		glTranslatef(x, y, z);

	} else {
		GLfloat* translationMatrix = new GLfloat[16];
		translationMatrix[0] = 1.0;
		translationMatrix[1] = 0.0;
		translationMatrix[2] = 0.0;
		translationMatrix[3] = 0.0;

		translationMatrix[4] = 0.0;
		translationMatrix[5] = 1.0;
		translationMatrix[6] = 0.0;
		translationMatrix[7] = 0.0;

		translationMatrix[8] = 0.0;
		translationMatrix[9] = 0.0;
		translationMatrix[10] = 1.0;
		translationMatrix[11] = 0.0;

		translationMatrix[12] = x;
		translationMatrix[13] = y;
		translationMatrix[14] = z;
		translationMatrix[15] = 1.0;

		loadMatrix(translationMatrix);
	}
}

void myGlRotatef(GLfloat angle, GLfloat x, GLfloat y, GLfloat z) {
	if (!callingMyGl) {
		glRotatef(angle, x, y, z);

	} else {
		GLfloat norm = sqrt(pow(x, 2) + pow(y, 2) + pow(z, 2));

		if (norm != 0) {
			GLfloat xx = x / norm;
			GLfloat yy = y / norm;
			GLfloat zz = z / norm;
			GLfloat mMatrix[] = {xx * xx, yy * xx, zz * xx, xx * yy, yy * yy, zz * yy, xx * zz, yy * zz, zz * zz};
			GLfloat iMatrix[] = {1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0};
			GLfloat sMatrix[] = {0.0, zz, -yy, -zz, 0.0, xx, yy, -xx, 0.0};
			GLfloat angleInRadians = angle / 180 * M_PI;
			GLfloat cosA = cos(angleInRadians);
			GLfloat sinA = sin(angleInRadians);

			for (int i = 0; i < 9; i++) {
				mMatrix[i] += cosA * (iMatrix[i] - mMatrix[i]) + sinA * sMatrix[i];
			}

			GLfloat* rotationMatrix = new GLfloat[16];
			rotationMatrix[0] = mMatrix[0];
			rotationMatrix[1] = mMatrix[1];
			rotationMatrix[2] = mMatrix[2];
			rotationMatrix[3] = 0.0;

			rotationMatrix[4] = mMatrix[3];
			rotationMatrix[5] = mMatrix[4];
			rotationMatrix[6] = mMatrix[5];
			rotationMatrix[7] = 0.0;

			rotationMatrix[8] = mMatrix[6];
			rotationMatrix[9] = mMatrix[7];
			rotationMatrix[10] = mMatrix[8];
			rotationMatrix[11] = 0.0;

			rotationMatrix[12] = 0.0;
			rotationMatrix[13] = 0.0;
			rotationMatrix[14] = 0.0;
			rotationMatrix[15] = 1.0;

			loadMatrix(rotationMatrix);
		}
	}
}

void myGlScalef(GLfloat x, GLfloat y, GLfloat z) {
	if (!callingMyGl) {
		glScalef(x, y, z);

	} else {
		GLfloat* scalingMatrix = new GLfloat[16];
		scalingMatrix[0] = x;
		scalingMatrix[1] = 0.0;
		scalingMatrix[2] = 0.0;
		scalingMatrix[3] = 0.0;

		scalingMatrix[4] = 0.0;
		scalingMatrix[5] = y;
		scalingMatrix[6] = 0.0;
		scalingMatrix[7] = 0.0;

		scalingMatrix[8] = 0.0;
		scalingMatrix[9] = 0.0;
		scalingMatrix[10] = z;
		scalingMatrix[11] = 0.0;

		scalingMatrix[12] = 0.0;
		scalingMatrix[13] = 0.0;
		scalingMatrix[14] = 0.0;
		scalingMatrix[15] = 1.0;

		loadMatrix(scalingMatrix);
	}
}

void myGlLoadIdentity() {
	if (!callingMyGl) {
		glLoadIdentity();

	} else {
		GLfloat* identityMatrix = new GLfloat[16];
		identityMatrix[0] = 1.0;
		identityMatrix[1] = 0.0;
		identityMatrix[2] = 0.0;
		identityMatrix[3] = 0.0;

		identityMatrix[4] = 0.0;
		identityMatrix[5] = 1.0;
		identityMatrix[6] = 0.0;
		identityMatrix[7] = 0.0;

		identityMatrix[8] = 0.0;
		identityMatrix[9] = 0.0;
		identityMatrix[10] = 1.0;
		identityMatrix[11] = 0.0;

		identityMatrix[12] = 0.0;
		identityMatrix[13] = 0.0;
		identityMatrix[14] = 0.0;
		identityMatrix[15] = 1.0;

		while (!myGlMatrixStack.empty()) {
			GLfloat* topMatrix = myGlMatrixStack.top();
			myGlMatrixStack.pop();

			delete[] topMatrix;
		}

		myGlMatrixStack.push(identityMatrix);

		glLoadMatrixf(identityMatrix);
	}
}

void normalizeVector(GLdouble* vector) {
	GLdouble norm = sqrt(pow(vector[0], 2) + pow(vector[1], 2) + pow(vector[2], 2));

	if (norm != 0) {
		vector[0] /= norm;
		vector[1] /= norm;
		vector[2] /= norm;
	}
}

void computeCrossProductOfVectors(GLdouble* u, GLdouble* v, GLdouble* n) {
	n[0] = u[1] * v[2] - u[2] * v[1];
	n[1] = u[2] * v[0] - u[0] * v[2];
	n[2] = u[0] * v[1] - u[1] * v[0];
}

void myGluLookAt(GLdouble eyeX, GLdouble eyeY, GLdouble eyeZ, GLdouble centerX, GLdouble centerY, GLdouble centerZ,
		GLdouble upX, GLdouble upY, GLdouble upZ) {

	if (!callingMyGl) {
		gluLookAt(eyeX, eyeY, eyeZ, centerX, centerY, centerZ, upX, upY, upZ);

	} else {
		GLdouble forwardVector[] = {centerX - eyeX, centerY - eyeY, centerZ - eyeZ};
		GLdouble upVector[] = {upX, upY, upZ};
		GLdouble sideVector[3];

		normalizeVector(forwardVector);
		normalizeVector(upVector);
		computeCrossProductOfVectors(forwardVector, upVector, sideVector);
		normalizeVector(sideVector);
		computeCrossProductOfVectors(sideVector, forwardVector, upVector);

		GLfloat* mMatrix = new GLfloat[16];
		mMatrix[0] = sideVector[0];
		mMatrix[1] = upVector[0];
		mMatrix[2] = -forwardVector[0];
		mMatrix[3] = 0.0;

		mMatrix[4] = sideVector[1];
		mMatrix[5] = upVector[1];
		mMatrix[6] = -forwardVector[1];
		mMatrix[7] = 0.0;

		mMatrix[8] = sideVector[2];
		mMatrix[9] = upVector[2];
		mMatrix[10] = -forwardVector[2];
		mMatrix[11] = 0.0;

		mMatrix[12] = 0.0;
		mMatrix[13] = 0.0;
		mMatrix[14] = 0.0;
		mMatrix[15] = 1.0;

		loadMatrix(mMatrix);
		myGlTranslatef(-eyeX, -eyeY, -eyeZ);
	}
}
