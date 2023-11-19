package projects.dao;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import projects.entity.Category;
import projects.entity.Material;
import projects.entity.Project;
import projects.entity.Step;
import projects.exception.DbException;
import provided.util.DaoBase;

/*
 *  ProjectDao should extend DaoBase. If it doesn't, do that now or you will run into 
 *  problems later. 
 */
@SuppressWarnings("unused")
public class ProjectDao extends DaoBase { 
	
	private static final String CATEGORY_TABLE = "category";
	private static final String MATERIAL_TABLE = "material";
	private static final String PROJECT_TABLE = "project";
	private static final String PROJECT_CATEGORY_TABLE = "project_category";
	private static final String STEP_TABLE = "step";
	/**
	 * 
	 * @param project The project object to insert.
	 * @return The Project object with the primary key.
	 * @throws DbException Thrown if any error occurs inserting the row.
	 */
	
	// Modifications to project dao
	
	/* Now you need to write the code to retrieve all the projects from the database. 
	 * It is structured similarly to the insertProject() method, but it will also 
	 * incorporate a ResultSet to retrieve the project row(s).  To implement this method, 
	 * first you will write the SQL statement that instructs MySQL to return all project 
	 * rows without any materials, steps, or categories. Then, you will obtain a Connection 
	 * and start a transaction. Next, you will obtain a PreparedStatement from the 
	 * Connection object. Then, you will get a ResultSet from the PreparedStatement. 
	 * Finally, you will iterate over the ResultSet to create a Project object for each row returned. 
	 * 
	 */
	
	public Project insertProject(Project project) {
		// TODO Auto-generated method stub
		
		
		// @formatter:off
		String sql = ""
			+ "INSERT INTO " + PROJECT_TABLE + " "
			+ "(project_name, estimated_hours, actual_hours, difficulty, notes) "
			+ "VALUES "
			+ "(?, ?, ?, ?, ?)";
		// @formatter:on
		
		try(Connection conn = DbConnection.getConnection()) {
	
			startTransaction(conn); // Transaction started
			try(PreparedStatement stmt = conn.prepareStatement(sql)){
				
				setParameter(stmt, 1, project.getProjectName(), String.class);
				setParameter(stmt, 2, project.getEstimatedHours(), BigDecimal.class);
				setParameter(stmt, 3, project.getActualHours(), BigDecimal.class);
				setParameter(stmt, 4, project.getDifficulty(), Integer.class);
				setParameter(stmt, 5, project.getNotes(), String.class);
				
				stmt.executeUpdate();
				
				Integer projectId = getLastInsertId(conn, PROJECT_TABLE);
				commitTransaction(conn);
				
				project.setProjectId(projectId);
				return project;
			}
			catch(Exception e) { // Transaction rolled back
				rollbackTransaction(conn);
				throw new DbException(e);
			}
		}
		catch(SQLException e) {
			throw new DbException(e);
			}
		}
	// Modifications to ProjectDao
	
	/*
	 * Now you need to write the code to retrieve all the projects from the database. It is structured similarly 
	 * to the insertProject() method, but it will also incorporate a ResultSet to retrieve the project row(s).

To implement this method, first you will write the SQL statement that instructs MySQL to return all project rows
without any materials, steps, or categories. Then, you will obtain a Connection and start a transaction. Next, 
you will obtain a PreparedStatement from the Connection object. Then, you will get a ResultSet from the PreparedStatement. 
Finally, you will iterate over the ResultSet to create a Project object for each row returned.

In this section, you will be working in ProjectDao.java.

1.     In the method fetchAllProjects():

	a.     Write the SQL statement to return all projects not including materials, steps, or categories. Order the results by project name.

	b.     Add a try-with-resource statement to obtain the Connection object. Catch the SQLException in a catch block and rethrow a new 
	DbException, passing in the SQLException object.

	c.      Inside the try block, start a new transaction.

	d.     Add an inner try-with-resource statement to obtain the PreparedStatement from the Connection object. In a catch block, catch an 
	Exception object. Rollback the transaction and throw a new DbException, passing in the Exception object as the cause.

	e.     Inside the (currently) innermost try-with-resource statement, add a try-with-resource statement to obtain a ResultSet from 
	the PreparedStatement. Include the import statement for ResultSet. It is in the java.sql package.

	f.      Inside the new innermost try-with-resource, create and return a List of Projects.

	g.     Loop through the result set. Create and assign each result row to a new Project object. Add the Project object to the List of 
	Projects. You can do this by calling the extract method:
	 */
	public List<Project> fetchAllProjects() {
		// TODO Auto-generated method stub
		String sql = "SELECT * FROM " + PROJECT_TABLE + " ORDER BY project_name";
		
		try(Connection conn = DbConnection.getConnection()) {
			startTransaction(conn);
			
			try(PreparedStatement stmt = conn.prepareStatement(sql)){
				try(ResultSet rs = stmt.executeQuery()) {
					List<Project> projects = new LinkedList<>();
					
					// Shorter method:
					/*while(rs.next()) {
						projects.add(extract(rs, Project.class));
					}*/
					
					
					// Alternate method:
					while(rs.next()) {
					  Project project = new Project();
					  
					  project.setActualHours(rs.getBigDecimal("actual_hours"));
					  project.setDifficulty(rs.getObject("difficulty", Integer.class));
					  project.setEstimatedHours(rs.getBigDecimal("estimated_hours"));
					  project.setNotes(rs.getString("notes"));
					  project.setProjectId(rs.getObject("project_id", Integer.class));
					  project.setProjectName(rs.getString("project_name"));
					  
					 projects.add(project);
					  }
					 
					return projects;
				}
			}
			catch(Exception e) {
				rollbackTransaction(conn);
				throw new DbException(e);	
			}
		}
		catch(SQLException e) {
			throw new DbException(e);
		}
	}

	/* Create method fetchProjectById(). It returns a Project object and takes an Integer projectId as a 
	 * parameter. Inside the method:
	 * 
	 *  Temporarily assign a variable of type Optional<Project> to the results of 
	 * calling projectDao.fetchProjectById(). Pass the project ID to the method.
	 */
	public Optional<Project> fetchProjectById(Integer projectId) {
		// TODO Auto-generated method stub
		
		// Modifications to Project Dao
		
		/*
		 * In this section you will write the code that will retrieve a project row and all associated 
		 * child rows: materials, steps, and categories. The method will start with the usual 
		 * try-with-resource statement to obtain the Connection. Then you will add a try/catch before 
		 * obtaining the PreparedStatement. This is done so that after obtaining the project details, 
		 * the materials, steps, and categories can be retrieved within the same transaction. 
		 */
		
		/*
		 * In the method fetchProjectById(): 
		 * 	a. 	Write the SQL statement to return all columns from the project table in the row that matches 
		 * the given projectId. Make sure to use the parameter placeholder "?" in the SQL statement. 
		 * 	b. 	Obtain a Connection object in a try-with-resource statement. Add the catch block to 
		 * handle the SQLException. In the catch block throw a new DbException passing the SQLException 
		 * object as a parameter. 
		 * 	c. 	Start a transaction inside the try-with-resource statement. 
		 * 	d. 	Below the method call to startTransaction(), add an inner try/catch. 
		 * The catch block should handle Exception. Inside the catch block, rollback the transaction and 
		 * throw a new DbException that takes the Exception object as a parameter. 
		 * 	e. 	Inside the try block, create a variable of type Project and set it to null. 
		 * Return the Project object as an Optional object using Optional.ofNullable(). Save the file. 
		 * You should have no compilation errors at this point but you may see some warnings. This is OK.
		 */
		String sql = "SELECT * FROM " + PROJECT_TABLE + " WHERE project_id = ?";
		
		try(Connection conn = DbConnection.getConnection()){
			startTransaction(conn);
			try {
				Project project = null;
				/*
				 * Inside the inner try block, obtain a PreparedStatement from the Connection object in a 
				 * try-with-resource statement. Pass the SQL statement in the method call to 
				 * prepareStatement(). Add the projectId method parameter as a parameter to the 
				 * PreparedStatement. 
				 */
				
				
				try(PreparedStatement stmt = conn.prepareStatement(sql)){
					setParameter(stmt, 1, projectId, Integer.class);
					
					try(ResultSet rs = stmt.executeQuery()){
						/*
						 * Obtain a ResultSet in a try-with-resource statement. If the ResultSet has a row in it 
						 * (rs.next()) set the Project variable to a new Project object and set all fields from 
						 * values in the ResultSet. You can call the extract() method for this.
						 */
						if(rs.next()) {
							project = extract(rs, Project.class);
						
							  }
					}
				}
			
				if(Objects.nonNull(project)) {
					/*
					 * Below the try-with-resource statement that obtains the PreparedStatement but inside the 
					 * try block that manages the rollback, add three method calls to obtain the list of materials, 
					 * steps, and categories. Since each method returns a List of the appropriate type, you can call 
					 * addAll() to add the entire List to the List in the Project object: 
					 */
					project.getMaterials().addAll(fetchMaterialsForProject(conn, projectId));
					project.getSteps().addAll(fetchStepsForProject(conn, projectId));
					project.getCategories().addAll(fetchCategoriesForProject(conn, projectId));
				}
				// commit the transaction.
				commitTransaction(conn);
				return Optional.ofNullable(project);				
		}
		catch(Exception e) {
			rollbackTransaction(conn);
			throw new DbException(e);
		}
	}
		catch(SQLException e ) {
			throw new DbException(e);
		}
	}
	/*
	 * Follow these instructions to write the three methods to return materials, steps, and categories. 
	 * Each method should return a List of the appropriate type. At this point there should be no 
	 * compilation errors. 
	 * a. Each method should take the Connection and the project ID as parameters. 
	 * b. Each method should return a List of the appropriate type (i.e., List<Material>).
	 * c. Each method is written in the same way as the other query methods with the exception 
	 * that the Connection is passed as a parameter, so you don't need to call 
	 * DbConnection.getConnection() to obtain it. 
	 * d. Each method can add throws SQLException to the method declaration. 
	 * This is because the method call to each method is within a try/catch block. e. 
	 * Here is a sample method (all three methods should have the identical structure). 
	 * However, when you fetch the categories, you will need to join with the project_category 
	 * join table as shown below. 
	 */
	private List<Category> fetchCategoriesForProject(Connection conn, 
			Integer projectId)throws SQLException {
		// @formatter:off
		String sql = ""
			+ "SELECT c.* FROM " + CATEGORY_TABLE + " c "
			+ "JOIN " + PROJECT_CATEGORY_TABLE + " pc USING (category_id) "
			+ "WHERE project_id = ?";
		// @formatter:on
		
		try(PreparedStatement stmt = conn.prepareStatement(sql)) {
			setParameter(stmt, 1, projectId, Integer.class);
			
			try(ResultSet rs = stmt.executeQuery()) {
				List<Category> categories = new LinkedList<>();
				
				while(rs.next()) {
					categories.add(extract(rs, Category.class));
				}
				return categories;
			}
		}
	}
	/*
	 * In this step you will write the methods that will return materials, steps, and categories as Lists. 
	 * Each method is structured similarly. Since the Connection object is passed into each method, 
	 * you won't have to obtain the Connection from DbConnection.getConnection().  
	 * Also, you won't need to add catch blocks to the try-with-resource statements because the caller 
	 * makes the method calls within a try block. It won't hurt to catch the SQLException and turn it into 
	 * an unchecked exception as you have been doing. But it won't hurt to simply declare the exception in 
	 * the method signature either.
	 */
	private List<Material> fetchMaterialsForProject(Connection conn, 
			Integer projectId) throws SQLException {
		
		String sql = "SELECT * FROM " + MATERIAL_TABLE + " WHERE project_id = ?";
		
		try(PreparedStatement stmt = conn.prepareStatement(sql)){
			setParameter(stmt, 1, projectId, Integer.class);
			try(ResultSet rs = stmt.executeQuery()) {
				List<Material> materials = new LinkedList<>();
				
				while(rs.next()) {
					materials.add(extract(rs, Material.class));
				}
				return materials;
			}
		}
	}
	private List<Step> fetchStepsForProject(Connection conn, Integer projectId) throws SQLException{
		String sql = "SELECT * FROM " + STEP_TABLE + " WHERE project_id = ?";
		
		try(PreparedStatement stmt = conn.prepareStatement(sql)) {
			setParameter(stmt, 1, projectId, Integer.class);
			
			try(ResultSet rs = stmt.executeQuery()) {
				List<Step> steps = new LinkedList<>();
				
				while(rs.next()) {
					steps.add(extract(rs, Step.class));
				}
				return steps;
			}
		}
	}
	/*
	 * In modifyProjectDetails(), write the SQL statement to modify the project details. 
	 * Do not update the project ID – it should be part of the WHERE clause. Remember 
	 * to use question marks as parameter placeholders.
	 */
	public boolean modifyProjectDetails(Project project) {
		// TODO Auto-generated method stub
		// @formatter:off
		String sql = ""
			+ "UPDATE " + PROJECT_TABLE + " SET "
			+ "project_name = ?, "
			+ "estimated_hours = ?, "
			+ "actual_hours = ?, "
			+ "difficulty = ?, "
			+ "notes = ? "
			+ "WHERE project_id = ?";
		// @formatter:on
		
		/*
		 * Obtain the Connection and PreparedStatement using the appropriate 
		 * try-withresource and catch blocks. Start and rollback a transaction as usual. 
		 * Throw a DbException from each catch block. 
		 */
		
		try(Connection conn = DbConnection.getConnection()){
			startTransaction(conn);
			
			try(PreparedStatement stmt = conn.prepareStatement(sql)) {
				setParameter(stmt, 1, project.getProjectName(), String.class);
				setParameter(stmt, 2, project.getEstimatedHours(), BigDecimal.class);
				setParameter(stmt, 3, project.getActualHours(), BigDecimal.class);
				setParameter(stmt, 4, project.getDifficulty(), Integer.class);
				setParameter(stmt, 5, project.getNotes(), String.class);
				setParameter(stmt, 6, project.getProjectId(), Integer.class);
				
				boolean modified = stmt.executeUpdate() == 1;
				commitTransaction(conn);
				
				return modified;
			}
			catch(Exception e) {
				rollbackTransaction(conn);
				throw new DbException(e);											
			}
		}
		catch(SQLException e) {
			throw new DbException(e);
		}
	// Changes to the project DAO
		
	/*
	 * The deleteProject() method in the DAO is very similar to the modifyProjectDetails() method. 
	 * You will first create the SQL DELETE statement. Then, you will obtain the Connection and 
	 * PreparedStatement, and set the project ID parameter on the PreparedStatement. Then, you 
	 * will call executeUpdate() and verify that the return value is 1, indicating a successful 
	 * deletion. Finally, you will commit the transaction and return success or failure. 
	 */
	
	}
	public boolean deleteProject(Integer projectId) {
		/*
		 * In the method deleteProject(): 
		 * a. 	Write the SQL DELETE statement. Remember to use the placeholder for the project ID 
		 * in the WHERE clause. 
		 * b. 	Obtain a Connection and a PreparedStatement. Start, commit, and rollback a 
		 * transaction in the appropriate sections. 
		 * c. 	Set the project ID parameter on the 
		 * PreparedStatement. 
		 * d. 	Return true from the menu if executeUpdate() returns 1. 
		 */
		String sql = "DELETE FROM " + PROJECT_TABLE + " WHERE project_id = ?";
		
		try(Connection conn = DbConnection.getConnection()) {
			startTransaction(conn);
			
			try(PreparedStatement stmt = conn.prepareStatement(sql)) {
				setParameter(stmt, 1, projectId, Integer.class);
				
				boolean deleted = stmt.executeUpdate() == 1;
				
				commitTransaction(conn);
				return deleted;
			}
			catch(Exception e) {
				rollbackTransaction(conn);
				throw new DbException(e);
			}
		}
		catch(SQLException e) {
			throw new DbException(e);
		}
	}
}


		
		
