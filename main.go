package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"strconv"
	"time"

	_ "github.com/lib/pq"

	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = ""
	dbname   = "postgres"
)

func main() {
	// Log time in microseconds and filenames with log messages.
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	// Create the table by hand:
	//   CREATE TABLE fruit (id SERIAL, name TEXT);

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"dbname=%s sslmode=disable",
		host, port, user, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	fmt.Println("Successfully connected!")

	log.Printf("server started on https://localhost:8443")
	log.Fatal(http.ListenAndServeTLS(":8443", "cert.pem", "key.pem",
		createMux(db)))
}

// createMux returns an HTTP router that serves HTTP requests for different
// routes.
func createMux(db *sql.DB) http.Handler {
	r := mux.NewRouter()

	api := r.PathPrefix("/api/v1/").Subrouter()
	api.HandleFunc("/fruits", listFruitHandler(db)).Methods(http.MethodGet)
	api.HandleFunc("/fruits", createFruitHandler(db)).Methods(http.MethodPost)
	api.HandleFunc("/fruits/{id}", getFruitHandler(db)).Methods(http.MethodGet)
	api.HandleFunc("/sleep", sleepHandler(db)).Methods(http.MethodGet)

	r.HandleFunc("/", homeHandler)

	return r
}

func homeHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusInternalServerError)
}

// curl --insecure -d "durian" -X POST https://localhost:8443/api/v1/fruits
func createFruitHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fruit, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "", http.StatusBadRequest)
			return
		}

		statement := "INSERT INTO fruit (name) VALUES ($1)"
		_, err = db.Exec(statement, string(fruit))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		fmt.Fprint(w, "ok")
	}
}

// curl --insecure https://localhost:8443/api/v1/fruits
func listFruitHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		rows, err := db.QueryContext(ctx,
			"select p.id, p.name from fruit as p;")
		if err != nil {
			http.Error(w, "", http.StatusInternalServerError)
			return
		}

		type fruit struct {
			ID   int    `json:"id"`
			Name string `json:"name"`
		}
		var fruits []fruit
		for rows.Next() {
			var id int
			var name string
			err = rows.Scan(&id, &name)
			if err != nil {
				break
			}
			fruits = append(fruits, fruit{ID: id, Name: name})
		}

		if closeErr := rows.Close(); closeErr != nil {
			http.Error(w, closeErr.Error(), http.StatusInternalServerError)
			return
		}

		// Check for row scan error.
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Check for errors during row iteration.
		if err = rows.Err(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		type response struct {
			Fruits []fruit `json:"fruits"`
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response{
			Fruits: fruits,
		})
	}
}

// curl --insecure https://localhost:8443/api/v1/fruits/1
func getFruitHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		id, err := strconv.Atoi(vars["id"])
		if err != nil {
			http.Error(w, "non numeric id", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		var name string
		err = db.QueryRowContext(ctx,
			"select p.name from fruit as p where p.id = $1;", id).Scan(&name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		type response struct {
			ID   int    `json:"id"`
			Name string `json:"name"`
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response{
			ID:   id,
			Name: name,
		})
	}
}

// curl --insecure -v https://localhost:8443/api/v1/sleep?d=4
func sleepHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		duration := 5
		if r.FormValue("d") != "" {
			var err error
			duration, err = strconv.Atoi(r.FormValue("d"))
			if err != nil {
				http.Error(w, "non numeric duration", http.StatusBadRequest)
				return
			}
		}

		log.Printf("call to sleep for %d seconds", duration)
		defer log.Print("finished call to sleep")

		// Restrict the amount of time this request can take.
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		// Simulate a long running query. If the user closes the HTTP connection
		// while the request is ongoing, or if the deadline is exceeded, the
		// postgres driver will cancel the query and the below call will return
		// an error.
		_, err := db.QueryContext(ctx, "select pg_sleep($1);", duration)
		if err != nil {
			log.Printf("query failed: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		fmt.Fprint(w, "ok")
	}
}
