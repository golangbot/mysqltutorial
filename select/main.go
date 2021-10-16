package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	username = "root"
	password = "password"
	hostname = "127.0.0.1:3306"
	dbname   = "ecommerce"
)

type product struct {
	name  string
	price int
}

func dsn(dbName string) string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, hostname, dbName)
}

func dbConnection() (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn(""))
	if err != nil {
		log.Printf("Error %s when opening DB\n", err)
		return nil, err
	}
	//defer db.Close()

	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	res, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+dbname)
	if err != nil {
		log.Printf("Error %s when creating DB\n", err)
		return nil, err
	}
	no, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when fetching rows", err)
		return nil, err
	}
	log.Printf("rows affected %d\n", no)

	db.Close()
	db, err = sql.Open("mysql", dsn(dbname))
	if err != nil {
		log.Printf("Error %s when opening DB", err)
		return nil, err
	}
	//defer db.Close()

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(time.Minute * 5)

	ctx, cancelfunc = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	err = db.PingContext(ctx)
	if err != nil {
		log.Printf("Errors %s pinging DB", err)
		return nil, err
	}
	log.Printf("Connected to DB %s successfully\n", dbname)
	return db, nil
}

func createProductTable(db *sql.DB) error {
	query := `CREATE TABLE IF NOT EXISTS product(product_id int primary key auto_increment, product_name text, 
        product_price int, created_at datetime default CURRENT_TIMESTAMP, updated_at datetime default CURRENT_TIMESTAMP)`
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	res, err := db.ExecContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when creating product table", err)
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when getting rows affected", err)
		return err
	}
	log.Printf("Rows affected when creating table: %d", rows)
	return nil
}

func insert(db *sql.DB, p product) error {
	query := "INSERT INTO product(product_name, product_price) VALUES (?, ?)"
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when preparing SQL statement", err)
		return err
	}
	defer stmt.Close()
	res, err := stmt.ExecContext(ctx, p.name, p.price)
	if err != nil {
		log.Printf("Error %s when inserting row into products table", err)
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when finding rows affected", err)
		return err
	}
	log.Printf("%d products created ", rows)
	prdID, err := res.LastInsertId()
	if err != nil {
		log.Printf("Error %s when getting last inserted product", err)
		return err
	}
	log.Printf("Product with ID %d created", prdID)
	return nil
}

func multipleInsert(db *sql.DB, products []product) error {
	query := "INSERT INTO product(product_name, product_price) VALUES "
	var inserts []string
	var params []interface{}
	for _, v := range products {
		inserts = append(inserts, "(?, ?)")
		params = append(params, v.name, v.price)
	}
	queryVals := strings.Join(inserts, ",")
	query = query + queryVals
	log.Println("query is", query)
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when preparing SQL statement", err)
		return err
	}
	defer stmt.Close()
	res, err := stmt.ExecContext(ctx, params...)
	if err != nil {
		log.Printf("Error %s when inserting row into products table", err)
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when finding rows affected", err)
		return err
	}
	log.Printf("%d products created simultaneously", rows)
	return nil
}

func selectPrice(db *sql.DB, productName string) (int, error) {
	log.Printf("Getting product price")
	query := `select product_price from product where product_name = ?`
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when preparing SQL statement", err)
		return 0, err
	}
	defer stmt.Close()
	var price int
	row := stmt.QueryRowContext(ctx, productName)
	if err := row.Scan(&price); err != nil {
		return 0, err
	}
	return price, nil
}

func selectProductsByPrice(db *sql.DB, minPrice int, maxPrice int) ([]product, error) {
	log.Printf("Getting products by price")
	query := `select product_name, product_price from product where product_price >= ? && product_price <= ?;`
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when preparing SQL statement", err)
		return []product{}, err
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx, minPrice, maxPrice)
	if err != nil {
		return []product{}, err
	}
	var products = []product{}
	for rows.Next() {
		var prd product
		if err := rows.Scan(&prd.name, &prd.price); err != nil {
			return []product{}, err
		}
		products = append(products, prd)
	}
	if err := rows.Err(); err != nil {
		return []product{}, err
	}
	return products, nil
}

func main() {
	db, err := dbConnection()
	if err != nil {
		log.Printf("Error %s when getting db connection", err)
		return
	}
	defer db.Close()
	log.Printf("Successfully connected to database")
	err = createProductTable(db)
	if err != nil {
		log.Printf("Create product table failed with error %s", err)
		return
	}
	p := product{
		name:  "iphone",
		price: 950,
	}
	err = insert(db, p)
	if err != nil {
		log.Printf("Insert product failed with error %s", err)
		return
	}

	p1 := product{
		name:  "Galaxy",
		price: 990,
	}
	p2 := product{
		name:  "iPad",
		price: 500,
	}
	err = multipleInsert(db, []product{p1, p2})
	if err != nil {
		log.Printf("Multiple insert failed with error %s", err)
		return
	}

	productName := "iphone"
	price, err := selectPrice(db, productName)
	switch {
	case err == sql.ErrNoRows:
		log.Printf("Product %s not found in DB", productName)
	case err != nil:
		log.Printf("Encountered err %s when fetching price from DB", err)
	default:
		log.Printf("Price of %s is %d", productName, price)
	}

	minPrice := 900
	maxPrice := 1000
	products, err := selectProductsByPrice(db, minPrice, maxPrice)
	if err != nil {
		log.Printf("Error %s when selecting product by price", err)
		return
	}
	for _, product := range products {
		log.Printf("Name: %s Price: %d", product.name, product.price)
	}
}
