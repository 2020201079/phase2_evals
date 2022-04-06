CREATE TABLE labs
(
    lab_id INT NOT NULL,
    lab_name VARCHAR(100) NOT NULL,
    lab_location enum('KCIS', 'VINDHYA', 'HIMALAYA', 'NILGIRI') NOT NULL,
    PRIMARY KEY (lab_id)
);

CREATE TABLE faculty
(
    faculty_id INT NOT NULL,
    fname VARCHAR(100) NOT NULL,
    lname VARCHAR(100) NOT NULL,
    labId INT NOT NULL,
    PRIMARY KEY (faculty_id),
    FOREIGN KEY (labId) REFERENCES labs(lab_id)
);

CREATE TABLE students
(
  rno INT NOT NULL,
  fname VARCHAR(100) NOT NULL,
  lname VARCHAR(100) NOT NULL,
  branch enum('CSE','ECE','CSD','CLD','CND','ECD','CHD') NOT NULL,
  cgpa FLOAT NOT NULL,
  facId INT NOT NULL,
  expense INT,
  PRIMARY KEY (rno),
  FOREIGN KEY (facId) REFERENCES faculty(faculty_id)
);
