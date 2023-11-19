
package projects;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;

import projects.entity.Project;
import projects.exception.DbException;
import projects.service.ProjectService;

import projects.dao.DbConnection;

public class ProjectsApp {
	
	private Scanner scanner = new Scanner (System.in);
	private Project curProject;
	private ProjectService projectService = new ProjectService();
	
	// @formatter:off
	private List<String> operations = List.of(
		"1) Add a project",
		"2) List project",
		"3) Select a project",
		"4) Update project details",
		"5) Delete a project"
					
	);
	// @formatter:on
	

	//Private instance variable of type ProjectService
	/*public ProjectsApp() {
		projectService = new ProjectService();
	}*/
	
	
	
	public static void main(String[] args) {
		new ProjectsApp().processUserSelections();
		DbConnection.getConnection();
	}
	
	private void processUserSelections() {
		// TODO Auto-generated method stub
		
		boolean done = false;				
		while(!done) {			
			try {
				int selection = getUserSelection();
				
				switch(selection) {
				 case -1: done = exitMenu();
				 	break;
				 
				 case 1: createProject();
				 	break;
				 	/*
				 	 * In this section you will add another option to the list of available options. 
				 	 * Then you will add another case to the switch method along with the method to 
				 	 * call the service to retrieve the list of projects.

					In this section, you will be working in ProjectsApp.java.

						1.     Add this line to the list of operations at the top of ProjectsApp.java:
							"2) List projects".

						2.     Add case 2 to the switch statement in processUserSelection(). In the case, 
						call method listProjects(). Don't forget to add the break statement.
				 	 */
				 
				 case 2: listProjects();
				 	break;
				 
				 /*
				  * 1. Add an instance variable of type Project named curProject. 
				  * 2. Add a new operation: "3) Select a project". 
				  * 3. Add a case to the switch to handle the operation. 
				  * Call method selectProject(). 
				  */
				 
				 case 3: selectProject();
				 	break;
				 	
				 case 4: updateProjectDetails();
				 	break;
				 
				 case 5: deleteProject();
				 break;
				 
				 default:
					 System.out.println("\n" + selection + " is not a valid selection. Try again.");
					 break;
			}
		}
		catch(Exception e) {
			System.out.println("\nError: " + e + " Try again.");		
			}
		}
	}
	private void deleteProject() {
		// TODO Auto-generated method stub
		/*
		 * In method deleteProject(): a. Call method listProjects(). 
		 * b. Ask the user to enter the ID of the project to delete. 
		 * c. Call projectService.deleteProject() and pass the project ID entered by the user. 
		 * d. Print a message stating that the project was deleted. 
		 * (If it wasn't deleted, an exception is thrown by the service class.) 
		 * e. Add a check to see if the project ID in the current project is the same as the 
		 * ID entered by the user. If so, set the value of curProject to null. 
		 * f. Have Eclipse create the deleteProject() method in the project service. 
		 * g. Save all files. At this point there should be no compilation errors. 
		 */
		listProjects();
		
		Integer projectId = getIntInput("Enter the ID of the project to delete");
		
		projectService.deleteProject(projectId);
		System.out.println("Project " + projectId + " was deleted successfully.");
		
		if(Objects.nonNull(curProject) && curProject.getProjectId().equals(projectId)) {
			curProject = null;
		}
		
		
	}

	private void updateProjectDetails() {
		// TODO Auto-generated method stub
		/*
		 * Check to see if curProject is null. If so, print a message 
		 * "\nPlease select a project." and return from the method. 
		 */
		if(Objects.isNull((curProject))) {
			System.out.println("\nPlease select a project.");
			return;
			/*
			 * For each field in the Project object, print a message along 
			 * with the current setting in curProject. 
			 */
		}
		String projectName = getStringInput("Enter the project name ["
				+ curProject.getProjectName() + "]");
		/*
		 * Create a new Project object. If the user input for a value is not null, add the value 
		 * to the Project object. If the value is null, add the value from curProject. Repeat for 
		 * all Project variables. 
		 */
		Project project = new Project();
		project.setProjectName(Objects.isNull(projectName) ? curProject.getProjectName() : projectName);
		/*
		 * Set the project ID field in the Project object to the value in the curProject object.
		 */
		project.setProjectId(curProject.getProjectId());
		/*
		 * Call projectService.modifyProjectDetails(). Pass the Project object as a parameter. 
		 * Let Eclipse create the method for you in ProjectService.java. 
		 */
		projectService.modifyProjectDetails(project);
		/*
		 * Reread the current project to pick up the changes by calling projectService.fetchProjectById(). 
		 * Pass the project ID obtained from curProject. 
		 */
		curProject = projectService.fetchProjectById(curProject.getProjectId());
		
	}

	private void selectProject() {
		// TODO Auto-generated method stub
		/*
		 * In this step you will create the method, selectProject(). 
		 * This method will list the project IDs and names so that the user can select a project ID. 
		 * Once the ID is entered, the service is called to return the project details. If successful, 
		 * the current project is set to the returned project. Follow these instructions to write 
		 * the method. 
		 */
		// a. Call listProjects() to print a List of Projects. 
		listProjects();
		/*
		 *  b. Collect a project ID from the user and assign it to an Integer variable named projectId. 
		 *  Prompt the user with "Enter a project ID to select a project". 
		 */
		Integer projectId = getIntInput("Enter a project ID to select a project");
		/*
		 * c. Set the instance variable curProject to null to unselect any currently selected project. 
		 * This is done in case the call to the service results in an exception being thrown. 
		 * Rather than leave the current project selected in that case, it is unselected first. 
		 */
		curProject = null;
		/*
		 * d. Call a new method, fetchProjectById() on the projectService object. The method should take a 
		 * single parameter, the project ID input by the user. It should return a Project object. 
		 * Assign the returned Project object to the instance variable curProject. Note that if 
		 * an invalid project ID is entered, projectService.fetchProjectById() will throw a 
		 * NoSuchElementException, which is handled by the catch block in processUserSelections(). 
		 */
		curProject = projectService.fetchProjectById(projectId);
	}



	/*
	 * 3.     Have Eclipse create the method listProjects(). It should take no parameters and should return 
	 * nothing. In the method:

		a.     Create a variable to hold a List of Projects named projects. Assign the variable the results 
		of a method call to projectService.fetchAllProjects().

		b.     Print "\nProjects:" (without quotes) to the console.

		c.      For each Project, print the ID and name separated by ": ". Indent each line with a couple of 
		spaces.

		d.     At this point, the method should look like this:
	 */
	private void listProjects() {
		// TODO Auto-generated method stub
		
		List<Project> projects = projectService.fetchAllProjects();
		
		System.out.println("\nProjects:");
		
		projects.forEach(project -> System.out
			.println("  " + project.getProjectId()
				+ ": " + project.getProjectName()));
		
	}

	
	private void createProject() {
		// TODO Auto-generated method stub

		String projectName = getStringInput("Enter the project name");
		BigDecimal estimatedHours = getDecimalInput("Enter the estimated hours");
		BigDecimal actualHours = getDecimalInput("Enter the actual hours");
		Integer difficulty = getIntInput("Enter the project difficulty (1-5)");
		String notes = getStringInput("Enter the project notes");
		
		Project project = new Project();
		
		project.setProjectName(projectName);
		project.setEstimatedHours(estimatedHours);
		project.setActualHours(actualHours);
		project.setDifficulty(difficulty);
		project.setNotes(notes);
		
		Project dbProject = projectService.addProject(project);
		
		System.out.println("You have successfully created project: " + dbProject);	
	}
	

	private BigDecimal getDecimalInput(String prompt) {
		// TODO Auto-generated method stub
		String input = getStringInput(prompt);
		
		if(Objects.isNull(input)) {
			return null;
		}
		try {
			// Create the BigDecimal object and set it to two decimal places (the scale).
			return new BigDecimal(input).setScale(2);
			// This will create a new BigDecimal object and set the number of decimal places (the scale) to 2.
		}
		catch(NumberFormatException e) {
			throw new DbException(input + "is not a valid number.");
		}
	}

	private boolean exitMenu() {
		// TODO Auto-generated method stub
		System.out.println("Exiting the menu.");		
		return true;
	}
	
	private int getUserSelection() {
		// TODO Auto-generated method stub
		printOperations();
		
		Integer input = getIntInput("Enter a menu selection");
		
		return Objects.isNull(input)? -1 : input;
	}
	
	private void printOperations() {
		// TODO Auto-generated method stub
		System.out.println("\nThese are the available selections. Press the Enter key to quit:");
		
		
		/* With Lambda expression */
		operations.forEach(line -> System.out.println(" " + line));
		
		
		/* With enhanced for loop */
		// for(String line : operations) {
		// System.out.println(" " + line);
		// }
		
		/*
		 * In this step, you will add code to print the current project when the available menu 
		 * selections are displayed to the user. To do this, find the method printOperations(). 
		 * At the bottom of method printOperations(), check if curProject is null. If null, 
		 * print a message: "\nYou are not working with a project.". Otherwise, print the message: 
		 * "\nYou are working with project: " + curProject. 
		 */
		if(Objects.isNull(curProject)) {
			System.out.println("\nYou are not working with a project.");
		}
		else {
			System.out.println("\nYou are working with project: " + curProject);
		}
	}
	
	private Integer getIntInput(String prompt) {
		// TODO Auto-generated method stub
		String input = getStringInput(prompt);
		
		if(Objects.isNull(input)) {
			return null;
		}
		try {
			return Integer.valueOf(input);
		}
		catch(NumberFormatException e) {
			throw new DbException(input + "is not a valid number.");
		}
	}
	
	private String getStringInput(String prompt) {
		// TODO Auto-generated method stub
		System.out.print(prompt + ": ");
		String input = scanner.nextLine();
		
		return input.isBlank() ? null : input.trim();
	}

}
