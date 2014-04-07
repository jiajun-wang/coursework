#include "shaderModule.h"

static bool usingGlsl = true;
static GLchar* vertexShaderSource, *fragmentShaderSource;
static GLuint vertexShader, fragmentShader, shaderProgram = 0;
static GLint vertexShaderCompiled = 0, fragmentShaderCompiled = 0, shaderLinked = 0;
static GLint lightingLocation = -1;
static bool glslLighting = false;

unsigned long getFileLength(std::ifstream &file) {
	if (!file.good()) {
		return 0;
	}

	file.tellg();
	file.seekg(0, std::ios::end);
	unsigned long length = file.tellg();
	file.seekg(std::ios::beg);

	return length;
}

int loadShaderSource(const char* fileName, GLchar*& shaderSource) {
	std::ifstream file;
	file.open(fileName, std::ios::in);

	if (!file) {
		return -1;
	}

	unsigned long length = getFileLength(file);

	if (length == 0) {
		return -2;
	}

	if (shaderSource != 0) {
		delete[] shaderSource;
	}

	shaderSource = (GLchar*) new char[length + 1];

	if (shaderSource == 0) {
		return -3;
	}

	shaderSource[length] = 0;
	unsigned int i = 0;

	while (file.good()) {
		shaderSource[i] = file.get();

		if (!file.eof()) {
			i++;
		}
	}

	shaderSource[i] = 0;
	file.close();

	return 0;
}

void compileShader(GLuint &shader, GLint* shaderCompiled) {
	glCompileShader(shader);
	glGetShaderiv(shader, GL_COMPILE_STATUS, shaderCompiled);

	if (!*shaderCompiled) {
		GLint length;
		glGetShaderiv(shader, GL_INFO_LOG_LENGTH, &length);

		GLchar* compileLog = (GLchar*) malloc(length);
		glGetShaderInfoLog(shader, length, &length, compileLog);
		std::cout << "shader compile log: " << compileLog << std::endl;

		free(compileLog);
		exit(1);
	}
}

bool checkGlslRunnable() {
	return usingGlsl && shaderProgram != 0 && shaderLinked;
}

int checkGlError(const char *file, int line) {
	int returnCode = 0;
	GLenum glError = glGetError();

	while (glError != GL_NO_ERROR) {
		const GLubyte* sError = gluErrorString(glError);

		if (sError) {
			std::cout << "gl error #" << glError << "(" << sError << ") " << " in file " << file << " at line: "
					<< line << std::endl;
		} else {
			std::cout << "gl error #" << glError << " (no message available)" << " in file " << file << " at line: "
					<< line << std::endl;
		}

		returnCode = 1;
		glError = glGetError();
	}

	return returnCode;
}

void beginShader() {
	if (checkGlslRunnable()) {
		glUseProgram(shaderProgram);

		if (lightingLocation != -1) {
			glUniform1i(lightingLocation, glslLighting ? 1 : 0);
		}

		checkGlError(__FILE__, __LINE__);
	}
}

void endShader() {
	if (checkGlslRunnable()) {
		glUseProgram(0);

		checkGlError(__FILE__, __LINE__);
	}
}

void initializeShader() {
	if (!usingGlsl) {
		GLfloat light_ambient[] = {1.0, 1.0, 1.0, 1.0};
		GLfloat light_diffuse[] = {1.0, 1.0, 1.0, 1.0};
		GLfloat light_specular[] = {1.0, 1.0, 1.0, 1.0};
		GLfloat material_ambient_diffuse[] = {1.0, 1.0, 1.0, 1.0};
		GLfloat material_specular[] = {1.0, 1.0, 1.0, 1.0};
		GLfloat material_shininess[] = {100.0};

		glLightfv(GL_LIGHT0, GL_AMBIENT, light_ambient);
		glLightfv(GL_LIGHT0, GL_DIFFUSE, light_diffuse);
		glLightfv(GL_LIGHT0, GL_SPECULAR, light_specular);
		glMaterialfv(GL_FRONT, GL_AMBIENT_AND_DIFFUSE, material_ambient_diffuse);
		glMaterialfv(GL_FRONT, GL_SPECULAR, material_specular);
		glMaterialfv(GL_FRONT, GL_SHININESS, material_shininess);
		glColorMaterial(GL_FRONT, GL_AMBIENT_AND_DIFFUSE);

		glEnable(GL_COLOR_MATERIAL);

	} else {
		loadShaderSource("src/cube/vertexShader.txt", vertexShaderSource);
		loadShaderSource("src/cube/fragmentShader.txt", fragmentShaderSource);

		GLenum glewError = glewInit();
		if (GLEW_OK != glewError) {
			std::cout << "glew error: " << glewGetErrorString(glewError) << std::endl;
			exit(1);
		}

		vertexShader = glCreateShader(GL_VERTEX_SHADER);
		fragmentShader = glCreateShader(GL_FRAGMENT_SHADER);

		glShaderSource(vertexShader, 1, (const GLchar **) &vertexShaderSource, NULL);
		glShaderSource(fragmentShader, 1, (const GLchar **) &fragmentShaderSource, NULL);

		compileShader(vertexShader, &vertexShaderCompiled);
		compileShader(fragmentShader, &fragmentShaderCompiled);

		shaderProgram = glCreateProgram();
		glAttachShader(shaderProgram, vertexShader);
		glAttachShader(shaderProgram, fragmentShader);
		glLinkProgram(shaderProgram);
		glGetProgramiv(shaderProgram, GL_LINK_STATUS, &shaderLinked);

		if (!shaderLinked) {
			GLint length;
			glGetProgramiv(shaderProgram, GL_INFO_LOG_LENGTH, &length);

			GLchar* linkLog = (GLchar*) malloc(length);
			glGetProgramInfoLog(shaderProgram, length, &length, linkLog);
			std::cout << "shader link log: " << linkLog << std::endl;

			free(linkLog);
			exit(1);
		}

		glslLighting = true;
		lightingLocation = glGetUniformLocation(shaderProgram, "lighting");
	}
}

void toggleLight() {
	if (!checkGlslRunnable()) {
		glIsEnabled(GL_LIGHTING) ? glDisable(GL_LIGHTING) : glEnable(GL_LIGHTING);

	} else {
		glslLighting = !glslLighting;
	}
}
