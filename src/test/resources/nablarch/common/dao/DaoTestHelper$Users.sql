FIND_USERS =
SELECT *
FROM DAO_USERS
WHERE NAME = ?
      AND USER_ID = ?

FIND_USERS_WHERE_ENTITY =
SELECT *
FROM DAO_USERS
WHERE NAME = :name
      AND USER_ID = :id

FIND_USERS_ALL =
SELECT *
FROM DAO_USERS
WHERE NAME LIKE ? ESCAPE '\'
ORDER BY USER_ID

FIND_USERS_ALL_WHERE_ENTITY =
SELECT *
FROM DAO_USERS
WHERE NAME LIKE :name%
ORDER BY USER_ID

FIND_USERS_ALL_NOT_COND =
SELECT *
FROM DAO_USERS
ORDER BY USER_ID

FIND_BY_ID =
SELECT *
FROM DAO_USERS
WHERE USER_ID = :id

FIND_BY_ID_WHERE_ARRAY =
SELECT *
FROM DAO_USERS
WHERE USER_ID = ?

FIND_BY_ID_WHERE_ENTITY =
SELECT *
FROM DAO_USERS
WHERE USER_ID = :id

FIND_ALL_USERS =
SELECT *
FROM DAO_USERS
ORDER BY USER_ID

