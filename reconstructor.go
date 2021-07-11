package main

import "C"
import (
	"bytes"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/test_driver"

	// _ "github.com/pingcap/parser/test_driver"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
)

//parse functions takes a statement and returns an ast node.
func parse(sql string) (*ast.StmtNode, error) {
	p := parser.New()

	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}
	return &stmtNodes[0], nil
}

type colX struct {
	colNames   []string
	tableNames []string
}
type fetchPtr struct {
	fetchedPtr ast.ExprNode
}

var scopeVar int = 0
var queryCols []string
var SearchString string = ""
var startAppendingCols bool = false
var isLikeQuery bool = false
var ctxvariable int = -1

func resetVars() {
	queryCols = nil
	SearchString = ""
	startAppendingCols = false
	ctxvariable = -1
	isLikeQuery = false
}

// var incount int = 0

// var ptr ast.ExprNode = &ast.MatchAgainst{}

func (v *colX) Enter(in ast.Node) (ast.Node, bool) {
	scopeVar += 1
	// fmt.Printf("sv_enter : %v\n", scopeVar)
	// fmt.Printf("scope : %T\n", in)
	// fmt.Printf("scope : %v\n", in)
	// fmt.Println("__________________")
	if va, ok := in.(*ast.MatchAgainst); ok {
		startAppendingCols = true
		ctxvariable = int(va.Modifier)
	}
	if _, ok := in.(*ast.PatternLikeExpr); ok {
		isLikeQuery = true
	}
	if name, ok := in.(*ast.ColumnName); ok {
		if startAppendingCols {
			// fmt.Printf("SEARCH COLUMN NAME %v\n", name.Name.L)
			queryCols = append(queryCols, name.Name.L)
		}
		v.colNames = append(v.colNames, name.Name.O)
	}
	if name, ok := in.(*test_driver.ValueExpr); ok && startAppendingCols && ctxvariable == 1 {
		// fmt.Printf("VALUE OF EXPRESSION : %v\n", name.GetDatumString())
		SearchString = name.GetDatumString()
	}
	if name, ok := in.(*ast.TableName); ok {
		v.tableNames = append(v.tableNames, name.Name.O)
	}
	// if name, ok := in.(*ast.MatchAgainst); ok {
	// 	for _, val := range name.ColumnNames {
	// 		fmt.Printf("%v\n", val.Name)
	// 	}
	// }
	return in, false
}
func makeInQuery(querystring string, querytype int) string {
	domain := "https://vpc-mysql-esoffload-2ae53yabkcyzvp3beuvxkoawsi.us-east-1.es.amazonaws.com" // e.g. https://my-domain.region.es.amazonaws.com
	index := "testing.article"
	primary_col := "article_id"
	endpoint := domain + "/" + index + "/" + "_search"
	region := "us-east-1"
	service := "es"
	colnameofquery := "article_content"
	var str bytes.Buffer
	str.WriteString(fmt.Sprintf("SELECT * from XYZ WHERE %v IN (", primary_col))
	checkthis := querystring
	json1 := ""
	if querytype == 1 {
		json1 = fmt.Sprintf(`{
			"_source":["article_id"],
			"query":{
				"match_phrase": {
					"%v":%v
				}
			},
			"size":9999
		}`, colnameofquery, checkthis)
	}
	if querytype == 2 {
		json1 = fmt.Sprintf(`{
			"_source":["article_id"],
			"query":{
				"wildcard": {
					"%v": "%v"
				}
			},
			"size":9999
		}`, colnameofquery, checkthis)
	}
	body := strings.NewReader(json1)
	credentials := credentials.NewEnvCredentials()
	signer := v4.NewSigner(credentials)
	client := &http.Client{}
	req, err := http.NewRequest(http.MethodGet, endpoint, body)
	if err != nil {
		log.Fatalln(err)
	}
	req.Header.Add("Content-Type", "application/json")
	signer.Sign(req, body, service, region, time.Now())
	resp, err := client.Do(req)
	if err != nil {
		fmt.Print(err)
	}

	bdy, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}
	jsonParsed, err := gabs.ParseJSON(bdy)
	if err != nil {
		fmt.Println(err)
	}
	var idlist []float64
	for _, child1 := range jsonParsed.S("hits", "hits").Children() {
		for _, child2 := range child1.S("_source").Children() {
			idlist = append(idlist, child2.Data().(float64))
		}
	}
	if len(idlist) == 0 {
		str.WriteString("-1")
	}
	count := 0
	for i, val := range idlist {
		count += 1
		str.WriteString(fmt.Sprint(val))
		if i < len(idlist)-1 {
			str.WriteString(",")
		}
	}
	// fmt.Println(count)
	str.WriteString(")")
	return str.String()
}
func (v *colX) Leave(in ast.Node) (ast.Node, bool) {
	// fmt.Printf("sv_exit : %v\n", scopeVar)
	// fmt.Printf("scope : %T\n", in)
	// fmt.Printf("scope : %v\n", in)
	scopeVar -= 1
	if _, ok := in.(*ast.MatchAgainst); ok && ctxvariable == 1 && SearchString[0] == '"' {
		queryString := makeInQuery(SearchString, 1)
		tempAstNode, err := parse(queryString)
		if err != nil {
			fmt.Printf("parse error: %v\n", err.Error())
			return in, true
		}
		resetVars()
		u := &fetchPtr{}
		u.fetchedPtr = &ast.PatternInExpr{}
		extractIDinPtr(u, tempAstNode)
		in = u.fetchedPtr
	}
	if v, ok := in.(*test_driver.ValueExpr); ok && isLikeQuery == true {
		// fmt.Println("^^^^^^^^^^^^^^^^")
		// fmt.Println(v.GetDatumString())
		// fmt.Println("^^^^^^^^^^^^^^^^")
		SearchString = v.GetDatumString()
	}
	if _, ok := in.(*ast.PatternLikeExpr); ok {
		if SearchString[0] == '%' && SearchString[len(SearchString)-1] == '%' {
			var temp bytes.Buffer
			for _, val := range SearchString {
				if val == '%' {
					temp.WriteString("*")
				} else if val == '_' {
					temp.WriteString("?")
				} else if (val != '*') && (val != '?') {
					temp.WriteString(string(val))
				}
			}
			queryString := makeInQuery(temp.String(), 2)
			tempAstNode, err := parse(queryString)
			if err != nil {
				fmt.Printf("parse error: %v\n", err.Error())
				return in, true
			}
			resetVars()
			u := &fetchPtr{}
			u.fetchedPtr = &ast.PatternInExpr{}
			extractIDinPtr(u, tempAstNode)
			in = u.fetchedPtr
		}
	}
	// if in1, ok := in.(*ast.MatchAgainst); ok {
	// 	ptr = in1
	// }
	// if _, ok := in.(*ast.PatternInExpr); ok {
	// 	in = ptr
	// }
	return in, true
}

func process_query(rootNode *ast.StmtNode) {
	v := &colX{}
	(*rootNode).Accept(v)
	return
}
func extractIDinPtr(u *fetchPtr, rootNode *ast.StmtNode) {
	(*rootNode).Accept(u)
}
func (v *fetchPtr) Enter(in ast.Node) (ast.Node, bool) {
	if name, ok := in.(*ast.PatternInExpr); ok {
		v.fetchedPtr = name
		return in, true
	}
	return in, false
}
func (v *fetchPtr) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

//export WrapperFunc
func WrapperFunc(sqlquery *C.char) *C.char {
	sql := C.GoString((*C.char)(sqlquery))
	astNode, _ := parse(sql)
	process_query(astNode)
	var sb strings.Builder
	val := *astNode
	val.Restore(format.NewRestoreCtx(265, &sb))
	return C.CString(sb.String())
}
func wf(sql string) string {
	astNode, _ := parse(sql)
	process_query(astNode)
	var sb strings.Builder
	val := *astNode
	val.Restore(format.NewRestoreCtx(265, &sb))
	return sb.String()
}
func main() {
	query := fmt.Sprintf(`SELECT article_id FROM article WHERE article_content LIKE "%%Asperiores%%"`)
	ans := wf(query)
	fmt.Println(ans)
}
