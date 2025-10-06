DROP TABLE IF EXISTS company;
CREATE TABLE company(
    company_cd varchar(5) NOT NULL,
    company_name varchar(256) NOT NULL,
    founded_year int NOT NULL,
    created_at timestamp NOT NULL,
    updated_at timestamp NOT NULL,
    revision int NOT NULL,
    CONSTRAINT company_pkc PRIMARY KEY(company_cd)
);

DROP TABLE IF EXISTS test_x;
CREATE TABLE test_x(
    id varchar(255) NOT NULL,
    a boolean NOT NULL,
    b blob NOT NULL,
    c char(1) NOT NULL,
    d date NOT NULL,
    e float NOT NULL,
    f double NOT NULL,
    g json NOT NULL,
    h json NOT NULL,
    i varchar(45) NOT NULL,
    j smallint NOT NULL,
    k int NOT NULL,
    l bigint NOT NULL,
    m time NOT NULL,
    n decimal(10,2) NOT NULL,
    o int NOT NULL,
    p text NOT NULL,
    q time NOT NULL,
    s timestamp NOT NULL,
    t timestamp NOT NULL,
    u varchar(36) NOT NULL,
    v varchar(255) NOT NULL,
    w smallint NOT NULL,
    x int NOT NULL,
    y bigint NOT NULL,
    z bit(1) NOT NULL,
    CONSTRAINT test_x_pkc PRIMARY KEY(id)
);

DROP TABLE IF EXISTS temperature;
CREATE TABLE temperature(
    ymd varchar(8) NOT NULL,
    value decimal(4,1) NOT NULL,
    CONSTRAINT temperature_pkc PRIMARY KEY(ymd)
);
