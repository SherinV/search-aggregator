package redisgraph

import (
	"crypto/rand"
	"fmt"
	"os"
	"strings"
	"strconv"


	"github.com/gomodule/redigo/redis"
	"github.com/olekukonko/tablewriter"
)

func quoteString(i interface{}) interface{} {
	switch x := i.(type) {
	case string:
		if len(x) == 0 {
			return "\"\""
		}
		if x[0] != '"' {
			x = "\"" + x
		}
		if x[len(x)-1] != '"' {
			x += "\""
		}
		return x
	default:
		return i
	}
}

// https://medium.com/@kpbird/golang-generate-fixed-size-random-string-dd6dbd5e63c0
func randomString(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	output := make([]byte, n)
	// We will take n bytes, one byte for each character of output.
	randomness := make([]byte, n)
	// read all random
	_, err := rand.Read(randomness)
	if err != nil {
		panic(err)
	}
	l := len(letterBytes)
	// fill output
	for pos := range output {
		// get random item
		random := uint8(randomness[pos])
		// random % 64
		randomPos := random % uint8(l)
		// put into output
		output[pos] = letterBytes[randomPos]
	}
	return string(output)
}

// Node represents a node within a graph.
type Node struct {
	ID         string
	Alias      string
	Label      string
	Properties map[string]interface{}
}

func (n *Node) String() string {
	s := []string{"("}
	if n.Alias != "" {
		s = append(s, n.Alias)
	}
	if n.Label != "" {
		s = append(s, ":", n.Label)
	}
	if len(n.Properties) > 0 {
		p := make([]string, 0, len(n.Properties))
		for k, v := range n.Properties {
			p = append(p, fmt.Sprintf("%s:%v", k, quoteString(v)))
		}
		s = append(s, "{")
		s = append(s, strings.Join(p, ","))
		s = append(s, "}")
	}
	s = append(s, ")")
	return strings.Join(s, "")
}

// Edge represents an edge connecting two nodes in the graph.
type Edge struct {
	Source      *Node
	Destination *Node
	Relation    string
	Properties  map[string]interface{}
}

func (e *Edge) String() string {
	s := []string{"(", e.Source.Alias, ")"}

	s = append(s, "-[")
	if e.Relation != "" {
		s = append(s, ":", e.Relation)
	}

	if len(e.Properties) > 0 {
		p := make([]string, 0, len(e.Properties))
		for k, v := range e.Properties {
			p = append(p, fmt.Sprintf("%s:%v", k, quoteString(v)))
		}
		s = append(s, strings.Join(p, ","))
		s = append(s, "{")
		s = append(s, p...)
		s = append(s, "}")
	}
	s = append(s, "]->")

	s = append(s, "(", e.Destination.Alias, ")")

	return strings.Join(s, "")
}

// Graph represents a graph, which is a collection of nodes and edges.
type Graph struct {
	Name  string
	Nodes map[string]*Node
	Edges []*Edge
	Conn  redis.Conn
}

// New creates a new graph.
func (g Graph) New(name string, conn redis.Conn) Graph {
	r := Graph{
		Name:  name,
		Nodes: make(map[string]*Node),
		Conn:  conn,
	}
	return r
}

// AddNode adds a node to the graph.
func (g *Graph) AddNode(n *Node) error {
	if n.Alias == "" {
		n.Alias = randomString(10)
	}
	g.Nodes[n.Alias] = n
	return nil
}

// AddEdge adds an edge to the graph.
func (g *Graph) AddEdge(e *Edge) error {
	// Verify that the edge has source and destination
	if e.Source == nil || e.Destination == nil {
		return fmt.Errorf("AddEdge: both source and destination nodes should be defined")
	}

	// Verify that the edge's nodes have been previously added to the graph
	if _, ok := g.Nodes[e.Source.Alias]; !ok {
		return fmt.Errorf("AddEdge: source node neeeds to be added to the graph first")
	}
	if _, ok := g.Nodes[e.Destination.Alias]; !ok {
		return fmt.Errorf("AddEdge: destination node neeeds to be added to the graph first")
	}

	g.Edges = append(g.Edges, e)
	return nil
}

// Commit creates the entire graph, but will readd nodes if called again.
func (g *Graph) Commit() (QueryResult, error) {
	items := make([]string, 0, len(g.Nodes)+len(g.Edges))
	for _, n := range g.Nodes {
		items = append(items, n.String())
	}
	for _, e := range g.Edges {
		items = append(items, e.String())
	}
	q := "CREATE " + strings.Join(items, ",")
	return g.Query(q)
}

// Flush will create the graph and clear it
func (g *Graph) Flush() (QueryResult, error) {
	res, err := g.Commit()
	if err == nil {
		g.Nodes = make(map[string]*Node)
		g.Edges = make([]*Edge, 0)
	}
	return res, err
}

// Query executes a query against the graph.
func (g *Graph) Query(q string) (QueryResult, error) {
	qr := QueryResult{}
	r, err := redis.Values(g.Conn.Do("GRAPH.QUERY", g.Name, q))
	if err != nil {
		return qr, err
	}

	// Result-set is an array of arrays.
	results, err := redis.Values(r[0], nil)
	if err != nil {
		return qr, err
	}

	records := make([][]interface{}, len(results))

	for i, result := range results {
		// Parse each record.
		records[i], err = redis.Values(result, nil)
		if err != nil {
			return qr, err
		}
	}

	// Convert each record item to string.
	qr.Results = make([][]string, len(records))
	for i, record := range records {
		qr.Results[i] = make([]string, len(record))
		for j, item := range record {
		    switch item.(type) {
		    case int64:
				n, err := redis.Int64(item, nil)
				if err != nil {
					return qr, err
				}
		        qr.Results[i][j] = strconv.FormatInt(n, 10)
		        break

		    case string:
		        qr.Results[i][j], err = redis.String(item, nil)
		        break

		    default:
				qr.Results[i][j], err = redis.String(item, nil)
		        break
		    }
		}
	}

	qr.Statistics, err = redis.Strings(r[1], nil)
	if err != nil {
		return qr, err
	}

	return qr, nil
}

// ExecutionPlan gets the execution plan for given query.
func (g *Graph) ExecutionPlan(q string) (string, error) {
	return redis.String(g.Conn.Do("GRAPH.EXPLAIN", g.Name, q))
}

func (g *Graph) Delete() error {
	_, err := g.Conn.Do("GRAPH.DELETE", g.Name)
	return err
}

// QueryResult represents the results of a query.
type QueryResult struct {
	Results    [][]string
	Statistics []string
}

func (qr *QueryResult) isEmpty() bool {
	return len(qr.Results) == 0
}

// PrettyPrint prints the QueryResult to stdout, pretty-like.
func (qr *QueryResult) PrettyPrint() {
	if !qr.isEmpty() {
		table := tablewriter.NewWriter(os.Stdout)
		table.SetAutoFormatHeaders(false)
		table.SetHeader(qr.Results[0])
		if len(qr.Results) > 1 {
			table.AppendBulk(qr.Results[1:])
		} else {
			table.Append([]string{"No data returned."})
		}
		table.Render()
	}

	for _, stat := range qr.Statistics {
		fmt.Fprintf(os.Stdout, "\n%s", stat)
	}

	fmt.Fprintf(os.Stdout, "\n")
}
