using System;
using System.Collections.Generic;
using System.Text;

namespace MongoCRUD
{
    public class QueryModel
    {
        public QueryModel()
        {
            PrimaryFields = new List<Field>();
            SecondaryFields = new List<Field>();
            Filters = new List<Filter>();
            SortBy = new List<Field>();
        }
        public List<Field> PrimaryFields
        {
            get; set;
        }
        public List<Field> SecondaryFields
        {
            get; set;
        }
        public List<Filter> Filters
        {
            get; set;
        }

        public List<Field> SortBy
        {
            get; set;
        }
    }

    public class Field
    {
        public string FieldName
        {
            get; set;
        }
        public string Measure
        {
            get; set;
        }
        public int OrderBy
        {
            get; set;
        }
        public int Priority
        {
            get; set;
        }
        public bool IsPrimary
        {
            get; set;
        }
    }

    public class Filter
    {
        public string FieldName
        {
            get; set;
        }
        public string Operation
        {
            get; set;
        }
        public List<object> FieldValue
        {
            get; set;
        }
        public string EndOperator
        {
            get; set;
        }
    }

    
}
