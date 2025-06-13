from __future__ import absolute_import

import sqlite3
from sqlite3 import Error
import os
import json

class NotebookVersion:
    def __init__(self, location):
        """Initialize NotebookVersion with the sciunit project location.
        
        Args:
            location (str): Path to the sciunit project directory
        """
        self.__fn = os.path.join(location, 'sciunit.db')
        self.__f = sqlite3.connect(self.__fn)
        self.__c = self.__f.cursor()
        
        # Create nbrewind table if it doesn't exist
        self.__f.executescript("""
        CREATE TABLE IF NOT EXISTS nbrewind (
            id      integer primary key not null,
            notebook    text not null unique,
            content text not null
        );
        """)
        self.__f.commit()

    def get_max_id(self):
        """Get the maximum ID from the nbrewind table.
        
        Returns:
            int: The maximum ID in the table, or 1 if the table is empty
        """
        script = "SELECT MAX(id) FROM revs"
        result = self.__c.execute(script).fetchone()[0]
        return result if result is not None else 1

    def get_all_versions(self):
        """Get all versions of notebook.
        
        """
        script = "SELECT notebook FROM nbrewind"
        result = self.__c.execute(script).fetchall()
        return result

    def persist(self, id, notebook_name, content):
        """Persist a notebook to the database.
        
        Args:
            notebook_name (str): The notebook
            content : notebok contents
            
        Returns:
            int: The ID assigned to the notebook
        """
        try:
            script = """
            INSERT INTO nbrewind (id, notebook, content)
            VALUES (?, ?, ?)
            """
            self.__c.execute(script, (id, notebook_name, content))
            self.__f.commit()
            return self.__c.lastrowid
        except Error as e:
            # if "UNIQUE constraint failed" in str(e):
            #     # If notebook already exists, return its ID
            #     return self.get_id(notebook_name)
            raise e

    def get_id(self, notebook_name):
        """Get the ID of a notebook based on its content.
        
        Args:
            notebook_content (str): The content of the notebook
            
        Returns:
            int: The ID of the notebook, or None if not found
        """
        script = """
        SELECT id FROM nbrewind 
        WHERE notebook = ?
        """
        result = self.__c.execute(script, (notebook_name,)).fetchone()
        return result[0] if result else None

    def get_notebook(self, notebook_id):
        """Get the notebook content based on its ID.
        
        Args:
            notebook_id (int): The ID of the notebook
            
        Returns:
            str: The notebook path, or None if not found
        """
        script = """
        SELECT notebook FROM nbrewind 
        WHERE id = ?
        """
        result = self.__c.execute(script, (notebook_id,)).fetchone()
        return result[0] if result else None

    def get_content(self, notebook_id):
        """Get the notebook content based on its ID.
        
        Args:
            notebook_id (int): The ID of the notebook
            
        Returns:
            str: The content of the notebook, or None if not found
        """
        script = """
        SELECT content FROM nbrewind 
        WHERE id = ?
        """
        result = self.__c.execute(script, (notebook_id,)).fetchone()
        return result[0] if result else None

    def close(self):
        """Close the database connection."""
        self.__f.close()