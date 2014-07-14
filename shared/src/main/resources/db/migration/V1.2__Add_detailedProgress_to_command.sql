/**
 * Add the column detailedProgress to command table
 * This column is used to display extra details of command progress
 */
ALTER TABLE command ADD COLUMN detailedProgress text NOT NULL DEFAULT '[]';