package projects.service;

import java.util.List;
import java.util.NoSuchElementException;

import projects.dao.ProjectDao;
import projects.entity.Project;
import projects.exception.DbException;

public class ProjectService {
	private ProjectDao projectDao = new ProjectDao();
	/*
	 * This method simply calls the DAO class to insert a project row.
	 * 
	 * @param project The {@link Project} object.
	 * @return The Project object with the newly generated primary key value.
	 */
	 
	public Project addProject(Project project) {
		// TODO Auto-generated method stub
		/*
		 * In method addProject(), call the method insertProject() on the projectDao object. 
		 * The method should take a single parameter. Pass it the Project parameter and return 
		 * the value from the method. The addProject() method should look like this: 
		 * public Project addProject(Project project) {   return projectDao.insertProject(project); } 
		 */
		return projectDao.insertProject(project);
	}
	/*
	 * In this section, you will be working in ProjectService.java.

		1.   In the method fetchAllProjects, call the fetchAllProjects() method on the projectDao object.

		2.   Have Eclipse create the method fetchAllProjects() in ProjectDao.java or create it yourself. 
		It takes no parameters and returns a List of Projects.
	 */
	
	public List<Project> fetchAllProjects() {
		// TODO Auto-generated method stub
		
		return projectDao.fetchAllProjects();
	}
	
	/* 
	 * Let Eclipse create the method for you in the ProjectDao class. 
	 * The editor will display ProjectDao.java. Return to ProjectService.java.       
	 */

	public Project fetchProjectById(Integer projectId) {
		// TODO Auto-generated method stub
		/*
		 * Add a method call to .orElseThrow() just inside the semicolon at the end of the method call to 
		 * projectDao.fetchProjectById(). Use a zero-argument Lambda expression inside the call to .orElseThrow() 
		 * to create and return a new NoSuchElementException with the custom message, 
		 * "Project with project ID=" + projectId + " does not exist."
		 */
		
		return projectDao.fetchProjectById(projectId).orElseThrow(() -> new NoSuchElementException(
				"Project with project ID=" + projectId + " does not exist."));
	}

	public void modifyProjectDetails(Project project) {
		// TODO Auto-generated method stub
		/*
		 * Call projectDao.modifyProjectDetails(). Pass the Project object as a parameter. 
		 * The DAO method returns a boolean that indicates whether the UPDATE operation was successful. 
		 * Check the return value. If it is false, throw a DbException with a message that says the project 
		 * does not exist. 
		 */
		if(!projectDao.modifyProjectDetails(project)) {
			throw new DbException("Project with ID="
				+ project.getProjectId() + " does not exist.");
		}
		
	}

	// Changes to the project service.
/*
 * The deleteProject() method in the service is very similar to the 
 * modifyProjectDetails() method. You will call the deleteProject() method in the DAO class and 
 * check the boolean return value. If the return value is false, a DbException is thrown with a message 
 * that the project with the given ID does not exist. The exception will be picked up by the exception 
 * handler in the application menu class. 
 */
	public void deleteProject(Integer projectId) {
		// TODO Auto-generated method stub
		/*
		 * 1. 	Call deleteProject() in the project DAO. Pass the project ID as a parameter. 
		 * The method returns a boolean. Test the return value from the method call. 
		 * If it returns false, throw a DbException with a message stating that the project doesn't exist.  
		 * 2. 	Have Eclipse create the deleteProject() method in the ProjectDao class. 
		 * 3. 	Save all files. At this point there should be no compilation errors. 
		 */
		if(!projectDao.deleteProject(projectId)) {
			throw new DbException("Project with ID =" + " does not exist.");
		}
	}

}
