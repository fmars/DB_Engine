#include <cstring>
#include <cstdlib>
#include <climits>
#include <limits>
#include <cassert>

#include "qe.h"

Iterator::~Iterator() {
	;
}

RC Iterator::readAttribute(const void* dataAux,
		const vector<Attribute> &recordDescriptor, const string attributeName,
		void *data) {
//	char dataAux[PAGE_SIZE] = { 0 };
//	readRecord(fileHandle, recordDescriptor, rid, dataAux);
	const char* p = (const char*) dataAux;
	unsigned int i = 0;

	while (i < recordDescriptor.size()
			&& recordDescriptor[i].name.compare(attributeName) != 0) {
		if (recordDescriptor[i].type == TypeInt)
			p += sizeof(int);
		else if (recordDescriptor[i].type == TypeReal)
			p += sizeof(float);
		else if (recordDescriptor[i].type == TypeVarChar)
			p += sizeof(int) + *(int*) p;
		i++;
	}
	if (i == recordDescriptor.size())
		return -1;
	if (recordDescriptor[i].type == TypeInt)
		memcpy(data, p, sizeof(int));
	else if (recordDescriptor[i].type == TypeReal)
		memcpy(data, p, sizeof(float));
	else if (recordDescriptor[i].type == TypeVarChar)
		memcpy(data, p, sizeof(int) + *(int*) p);
	return 0;
}

Filter::Filter(Iterator* input, const Condition &condition) {
	this->input = input;
	this->condition = condition;
	aux1 = 0;
}

Filter::~Filter() {
	;
}

Project::~Project() {
	;
}

Project::Project(Iterator *input, const vector<string> &attrNames) {
	this->input = input;
	this->attrNames = attrNames;
	aux1 = 0;
}

RC Project::getNextTuple(void *data) {
	char mdata[PAGE_SIZE] = { 0 };
	char *p = (char*) data;
	if (input->getNextTuple(mdata) == QE_EOF) {
//		printf("Debug: Project reached EOF (%d)\n", aux1);
		return QE_EOF;
	}
	vector<Attribute> attrs;
	input->getAttributes(attrs);
	for (unsigned int i = 0; i < attrNames.size(); i++) {
		for (unsigned int j = 0; j < attrs.size(); j++)
			if (attrNames[i].compare(attrs[j].name) == 0) {
				char tdata[PAGE_SIZE] = { 0 };
				readAttribute(mdata, attrs, attrNames[i], tdata);
				if (attrs[j].type == TypeInt) {
					*(int*) p = *(int*) tdata;
					p += sizeof(int);
				} else if (attrs[j].type == TypeReal) {
					*(int*) p = *(int*) tdata;
					p += sizeof(int);
				} else if (attrs[j].type == TypeVarChar) {
					*(int*) p = *(int*) tdata;
					p += sizeof(int);
					for (int k = 0; k < *(int*) tdata; k++)
						*p++ = tdata[sizeof(int) + k];
				}
				break;
			}
	}
	aux1 += 1;
	return 0;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
	vector<Attribute> mattrs;
	input->getAttributes(mattrs);
	for (unsigned int i = 0; i < attrNames.size(); i++) {
		for (unsigned int j = 0; j < mattrs.size(); j++)
			if (attrNames[i].compare(mattrs[j].name) == 0)
				attrs.push_back(mattrs[j]);
	}
}

static bool checkValue(const AttrType& type, const CompOp& compOp,
		const void* data, const void* value);

RC Filter::getNextTuple(void* data) {
	char dataAttr[PAGE_SIZE];
	vector<Attribute> attrs;
	map<string, Attribute> mapAttr;

	if (condition.bRhsIsAttr)
		return -1;
	getAttributes(attrs);
	for (unsigned int j = 0; j < attrs.size(); ++j)
		mapAttr[attrs.at(j).name] = attrs.at(j);
	if (mapAttr[condition.lhsAttr].type != condition.rhsValue.type)
		return -1;
	for (; true;) {
		if (input->getNextTuple(data) == QE_EOF) {
//			printf("Debug: Filter reached EOF (%d)\n", aux1);
			return QE_EOF;
		}
		readAttribute(data, attrs, condition.lhsAttr, dataAttr);
		if (checkValue(mapAttr[condition.lhsAttr].type, condition.op, dataAttr,
				condition.rhsValue.data))
			break;
	}
	aux1 += 1;
	return 0;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
	input->getAttributes(attrs);
}

static bool checkValue(const AttrType& type, const CompOp& compOp,
		const void* data, const void* value) {
	switch (type) {
	case TypeInt:
		switch (compOp) {
		case EQ_OP:
			if (*(int*) data == *(int*) value)
				return true;
			break;
		case LT_OP:
			if (*(int*) data < *(int*) value)
				return true;
			break;
		case GT_OP:
			if (*(int*) data > *(int*) value)
				return true;
			break;
		case LE_OP:
			if (*(int*) data <= *(int*) value)
				return true;
			break;
		case GE_OP:
			if (*(int*) data >= *(int*) value)
				return true;
			break;
		case NE_OP:
			if (*(int*) data != *(int*) value)
				return true;
			break;
		case NO_OP:
			return true;
		}
		break;
	case TypeReal:
		switch (compOp) {
		case EQ_OP:
			if (*(float*) data == *(float*) value)
				return true;
			break;
		case LT_OP:
			if (*(float*) data < *(float*) value)
				return true;
			break;
		case GT_OP:
			if (*(float*) data > *(float*) value)
				return true;
			break;
		case LE_OP:
			if (*(float*) data <= *(float*) value)
				return true;
			break;
		case GE_OP:
			if (*(float*) data >= *(float*) value)
				return true;
			break;
		case NE_OP:
			if (*(float*) data != *(float*) value)
				return true;
			break;
		case NO_OP:
			return true;
		}
		break;
	case TypeVarChar:
		*((char*) data + *(int*) data + sizeof(int)) = '\0';
		*((char*) value + *(int*) value + sizeof(int)) = '\0';

		string strData((char*) data + sizeof(int));
		string strValue((char*) value + sizeof(int));

		switch (compOp) {
		case EQ_OP:
			if (strData == strValue)
				return true;
			break;
		case LT_OP:
			if (strData < strValue)
				return true;
			break;
		case GT_OP:
			if (strData > strValue)
				return true;
			break;
		case LE_OP:
			if (strData <= strValue)
				return true;
			break;
		case GE_OP:
			if (strData >= strValue)
				return true;
			break;
		case NE_OP:
			if (strData != strValue)
				return true;
			break;
		case NO_OP:
			return true;
		}
		break;
	}
	return false;
}

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn,
		const Condition &condition, const unsigned numPages) {
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->condition = condition;
	flag = 0;
	vector<Attribute> attrs;
	leftIn->getAttributes(attrs);
	for (unsigned int i = 0; i < attrs.size(); i++)
		if (attrs[i].name.compare(condition.lhsAttr) == 0) {
			attr = attrs[i];
			break;
		}
}

INLJoin::~INLJoin() {
	;
}

RC INLJoin::getNextTuple(void *data) {
	char rightData[PAGE_SIZE] = { 0 };

	if (flag == 0) {
		if (leftIn->getNextTuple(leftData) == QE_EOF)
			return QE_EOF;
		flag = 1;
	}

	while (rightIn->getNextTuple(rightData) != QE_EOF) {
		vector<Attribute> leftAttrs;
		vector<Attribute> rightAttrs;
		leftIn->getAttributes(leftAttrs);
		rightIn->getAttributes(rightAttrs);
		char leftAttrData[PAGE_SIZE] = { 0 };
		char rightAttrData[PAGE_SIZE] = { 0 };
		readAttribute(leftData, leftAttrs, condition.lhsAttr, leftAttrData);
		readAttribute(rightData, rightAttrs, condition.rhsAttr, rightAttrData);

		if (checkValue(attr.type, condition.op, leftAttrData, rightAttrData)) {
			unsigned int l1 = getRecordLength(leftAttrs, leftData);
			unsigned int l2 = getRecordLength(rightAttrs, rightData);
			memcpy(data, leftData, l1);
			memcpy((char*) data + l1, rightData, l2);
			return 0;
		}
	}

	while (leftIn->getNextTuple(leftData) != QE_EOF) {
		rightIn->setIterator(NULL, NULL, true, true);
		while (rightIn->getNextTuple(rightData) != QE_EOF) {
			vector<Attribute> leftAttrs;
			vector<Attribute> rightAttrs;
			leftIn->getAttributes(leftAttrs);
			rightIn->getAttributes(rightAttrs);
			char leftAttrData[PAGE_SIZE] = { 0 };
			char rightAttrData[PAGE_SIZE] = { 0 };
			readAttribute(leftData, leftAttrs, condition.lhsAttr, leftAttrData);
			readAttribute(rightData, rightAttrs, condition.rhsAttr,
					rightAttrData);

			if (checkValue(attr.type, condition.op, leftAttrData,
					rightAttrData)) {
				unsigned int l1 = getRecordLength(leftAttrs, leftData);
				unsigned int l2 = getRecordLength(rightAttrs, rightData);
				memcpy(data, leftData, l1);
				memcpy((char*) data + l1, rightData, l2);
				return 0;
			}
		}
	}
	return QE_EOF;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const {
	leftIn->getAttributes(attrs);
	vector<Attribute> rightAttrs;
	rightIn->getAttributes(rightAttrs);
	attrs.insert(attrs.end(), rightAttrs.begin(), rightAttrs.end());
}

unsigned int Iterator::getRecordLength(
		const vector<Attribute> &recordDescriptor, const void *data) {
	int length = 0; // = (1 + recordDescriptor.size()) * sizeof(unsigned);
	const char* p = (const char*) data;

	for (unsigned int i = 0; i < recordDescriptor.size(); i++) {
		if (recordDescriptor[i].type == TypeInt) {
			length += sizeof(int);
			p += sizeof(int);
		} else if (recordDescriptor[i].type == TypeReal) {
			length += sizeof(float);
			p += sizeof(float);
		} else if (recordDescriptor[i].type == TypeVarChar) {
			int l = *(int*) p;
			length += sizeof(int) + l;
			p += sizeof(int) + l;
		}
	}
	return length;
}

NLJoin::NLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition,
		const unsigned numPages) :
		numPages(numPages), leftDataRead(false) {
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->condition = condition;
	leftIn->getAttributes(attrsLt);
	rightIn->getAttributes(attrsRt);
	for (unsigned int j = 0; j < attrsLt.size(); ++j)
		mapAttrLt[attrsLt.at(j).name] = attrsLt.at(j);
	for (unsigned int j = 0; j < attrsRt.size(); ++j)
		mapAttrRt[attrsRt.at(j).name] = attrsRt.at(j);
}

NLJoin::~NLJoin() {
	;
}

RC NLJoin::getNextTuple(void *data) {
	if (!condition.bRhsIsAttr)
		return -1;
	if (mapAttrLt[condition.lhsAttr].type != mapAttrRt[condition.rhsAttr].type)
		return -1;

	AttrType typeCondition = mapAttrRt[condition.rhsAttr].type;
	char rightData[PAGE_SIZE];
	char dataAttrLt[PAGE_SIZE], dataAttrRt[PAGE_SIZE];
	bool found;

	if (!leftDataRead) {
		if (leftIn->getNextTuple(leftData) != QE_EOF)
			leftDataRead = true;
		else
			return QE_EOF;
	}
	for (found = false;;) {
		while (rightIn->getNextTuple(rightData) != QE_EOF) {
			// If data from both sides meet the condition
			readAttribute(leftData, attrsLt, mapAttrLt[condition.lhsAttr].name,
					dataAttrLt);
			readAttribute(rightData, attrsRt, mapAttrRt[condition.rhsAttr].name,
					dataAttrRt);
			if (checkValue(typeCondition, condition.op, dataAttrLt,
					dataAttrRt)) {
				found = true;
				break;
			}
		}
		if (found)
			break;
		if (leftIn->getNextTuple(leftData) != QE_EOF)
			rightIn->setIterator();
		else
			break;
	}
	if (!found)
		return QE_EOF;

	unsigned int sizeLt = getRecordLength(attrsLt, leftData);
	unsigned int sizeRt = getRecordLength(attrsRt, rightData);
	char* p = (char*) data;

	memcpy(p, leftData, sizeLt);
	p += sizeLt;
	memcpy(p, rightData, sizeRt);
	return 0;
}

void NLJoin::getAttributes(vector<Attribute> &attrs) const {
	vector<Attribute> attrsLt;
	vector<Attribute> attrsRt;

	leftIn->getAttributes(attrsLt);
	rightIn->getAttributes(attrsRt);
	for (unsigned int j = 0; j < attrsLt.size(); ++j)
		attrs.push_back(attrsLt.at(j));
	for (unsigned int j = 0; j < attrsRt.size(); ++j)
		attrs.push_back(attrsRt.at(j));
}

Aggregate::~Aggregate() {
	for (unsigned int j = 0; j < aggData.size(); ++j) {
		delete aggData.at(j);
	}
	vector<char*>().swap(aggData);
}

const int Aggregate::TYPE_INT_MINIMUM = INT_MIN;
const int Aggregate::TYPE_INT_MAXIMUM = INT_MAX;
const float Aggregate::TYPE_REAL_NINF = -numeric_limits<float>::infinity();
const float Aggregate::TYPE_REAL_PINF = +numeric_limits<float>::infinity();
const char Aggregate::TYPE_VAR_CHAR_N[4] = { 0 };
const char Aggregate::TYPE_VAR_CHAR_P[5] = { 1, 0, 0, 0, CHAR_MAX };

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op) :
		posData(0) {
	valid = false;

	vector<Attribute>().swap(this->attrs);
	Attribute resAttr;
	string resAttrName;

	// Make up the vector of Attribute
	getOpStr(resAttrName, op);
	resAttrName.append("(").append(aggAttr.name).append(")");
	resAttr.name = resAttrName;
	switch (op) {
	case MIN:
	case MAX:
	case SUM:
	case AVG:
		resAttr.type = aggAttr.type;
		resAttr.length = aggAttr.length;
		break;
	case COUNT:
		resAttr.type = TypeInt;
		resAttr.length = sizeof(int);
		break;
	}

	char itData[PAGE_SIZE];
	vector<Attribute> itAttrs;
	char attrData[PAGE_SIZE] = { 0 };
	char aggrMIN[PAGE_SIZE];
	char aggrMAX[PAGE_SIZE];
	char aggrSUM[16]; // More than the max length of any possible primitive type
	int aggrCOUNT = 0;

	// Initialize the aggregation variables
	switch (op) {
	case MIN:
	case MAX:
		switch (aggAttr.type) {
		case TypeInt:
			*(int*) aggrMIN = TYPE_INT_MAXIMUM;
			*(int*) aggrMAX = TYPE_INT_MINIMUM;
			break;
		case TypeReal:
			*(float*) aggrMIN = TYPE_REAL_PINF;
			*(float*) aggrMAX = TYPE_REAL_NINF;
			break;
		case TypeVarChar:
			memcpy(aggrMIN, TYPE_VAR_CHAR_P, sizeof(TYPE_VAR_CHAR_P));
			memcpy(aggrMAX, TYPE_VAR_CHAR_N, sizeof(TYPE_VAR_CHAR_N));
			break;
		}
		break;
	case SUM:
	case AVG:
		switch (aggAttr.type) {
		case TypeInt:
			*(int*) aggrSUM = 0;
			break;
		case TypeReal:
			*(float*) aggrSUM = 0.0;
			break;
		default:
			return; // Cannot do SUM or AVG on TypeVarChar
		}
		break;
	default:
		break;
	}

	// Iterate through the input and do aggregation
	input->getAttributes(itAttrs);
	while (input->getNextTuple(itData) != QE_EOF) {
		if (readAttribute(itData, itAttrs, aggAttr.name, attrData) < 0)
			return;
		switch (op) {
		case MIN:
			if (checkValue(aggAttr.type, LT_OP, attrData, aggrMIN)) {
				switch (aggAttr.type) {
				case TypeInt:
					*(int*) aggrMIN = *(int*) attrData;
					break;
				case TypeReal:
					*(float*) aggrMIN = *(float*) attrData;
					break;
				case TypeVarChar:
					memcpy(aggrMIN, attrData, *(int*) attrData + sizeof(int));
					break;
				}
			}
			break;
		case MAX:
			if (checkValue(aggAttr.type, GT_OP, attrData, aggrMAX)) {
				switch (aggAttr.type) {
				case TypeInt:
					*(int*) aggrMAX = *(int*) attrData;
					break;
				case TypeReal:
					*(float*) aggrMAX = *(float*) attrData;
					break;
				case TypeVarChar:
					memcpy(aggrMAX, attrData, *(int*) attrData + sizeof(int));
					break;
				}
			}
			break;
		case SUM:
		case AVG:
			switch (aggAttr.type) {
			case TypeInt:
				*(int*) aggrSUM += *(int*) attrData;
				break;
			case TypeReal:
				*(float*) aggrSUM += *(float*) attrData;
				break;
			default:
				return;
			}
			break;
		default:
			break;
		}
		aggrCOUNT += 1;
	}

	void* pushData;

	// Save the correct data to an allocated memory space
	switch (op) {
	case MIN:
		switch (aggAttr.type) {
		case TypeInt:
			pushData = malloc(sizeof(int));
			*(int*) pushData = *(int*) aggrMIN;
			break;
		case TypeReal:
			pushData = malloc(sizeof(float));
			*(float*) pushData = *(float*) aggrMIN;
			break;
		case TypeVarChar:
			pushData = malloc(*(int*) aggrMIN + sizeof(int));
			memcpy(pushData, aggrMIN, *(int*) aggrMIN + sizeof(int));
			break;
		}
		break;
	case MAX:
		switch (aggAttr.type) {
		case TypeInt:
			pushData = malloc(sizeof(int));
			*(int*) pushData = *(int*) aggrMAX;
			break;
		case TypeReal:
			pushData = malloc(sizeof(float));
			*(float*) pushData = *(float*) aggrMAX;
			break;
		case TypeVarChar:
			pushData = malloc(*(int*) aggrMAX + sizeof(int));
			memcpy(pushData, aggrMAX, *(int*) aggrMAX + sizeof(int));
			break;
		}
		break;
	case SUM:
		if (aggAttr.type == TypeInt) {
			pushData = malloc(sizeof(int));
			*(int*) pushData = *(int*) aggrSUM;
		} else if (aggAttr.type == TypeReal) {
			pushData = malloc(sizeof(float));
			*(float*) pushData = *(float*) aggrSUM;
		}
		break;
	case AVG:
		if (aggAttr.type == TypeInt) { // AVG always returns float
			pushData = malloc(sizeof(int));
			*(float*) pushData = (float) *(int*) aggrSUM / aggrCOUNT;
		} else if (aggAttr.type == TypeReal) {
			pushData = malloc(sizeof(float));
			*(float*) pushData = *(float*) aggrSUM / aggrCOUNT;
		}
		break;
	case COUNT:
		pushData = malloc(sizeof(int));
		*(int*) pushData = aggrCOUNT;
	}

	// And push it into aggData
	this->aggData.push_back((char*) pushData);

	// End of aggregation
	attrs.push_back(resAttr);
	valid = true;
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, Attribute gAttr,
		AggregateOp op) :
		posData(0) {
	valid = false;

	vector<Attribute>().swap(this->attrs);
	Attribute resAttr;
	string resAttrName;

	// Make up the vector of Attribute
	getOpStr(resAttrName, op);
	resAttrName.append("(").append(aggAttr.name).append(")");
	resAttr.name = resAttrName;
	switch (op) {
	case MIN:
	case MAX:
	case SUM:
	case AVG:
		resAttr.type = aggAttr.type;
		resAttr.length = aggAttr.length;
		break;
	case COUNT:
		resAttr.type = TypeInt;
		resAttr.length = sizeof(int);
		break;
	}

	char itData[PAGE_SIZE];
	vector<Attribute> itAttrs;
	char attrData[PAGE_SIZE] = { 0 };
	char gData[PAGE_SIZE] = { 0 };
	char aggrMINDef[PAGE_SIZE];
	char aggrMAXDef[PAGE_SIZE];
	char aggrSUMDef[16]; // More than the max length of any possible primitive type
	const int aggrCOUNTDef = 0;
	vector<Buffer> aggrKey;
	vector<Buffer> aggrMIN;
	vector<Buffer> aggrMAX;
	vector<Buffer> aggrSUM;
	vector<int> aggrCOUNT;
	const Buffer emptyBuffer = { 0 };

	// Initialize the aggregation variables
	switch (op) {
	case MIN:
	case MAX:
		switch (aggAttr.type) {
		case TypeInt:
			*(int*) aggrMINDef = TYPE_INT_MAXIMUM;
			*(int*) aggrMAXDef = TYPE_INT_MINIMUM;
			break;
		case TypeReal:
			*(float*) aggrMINDef = TYPE_REAL_PINF;
			*(float*) aggrMAXDef = TYPE_REAL_NINF;
			break;
		case TypeVarChar:
			memcpy(aggrMINDef, TYPE_VAR_CHAR_P, sizeof(TYPE_VAR_CHAR_P));
			memcpy(aggrMAXDef, TYPE_VAR_CHAR_N, sizeof(TYPE_VAR_CHAR_N));
			break;
		}
		break;
	case SUM:
	case AVG:
		switch (aggAttr.type) {
		case TypeInt:
			*(int*) aggrSUMDef = 0;
			break;
		case TypeReal:
			*(float*) aggrSUMDef = 0.0;
			break;
		default:
			return; // Cannot do SUM or AVG on TypeVarChar
		}
		break;
	default:
		break;
	}

	map<int, int> aggrLookUpInt;
	map<float, int> aggrLookUpFloat;
	map<string, int> aggrLookUpStr;

	// Iterate through the input and do aggregation
	input->getAttributes(itAttrs);
	while (input->getNextTuple(itData) != QE_EOF) {
		if (readAttribute(itData, itAttrs, aggAttr.name, attrData) < 0)
			return;
		if (readAttribute(itData, itAttrs, gAttr.name, gData) < 0)
			return;

		unsigned int luInd;

		assert(aggrKey.size() == aggrCOUNT.size());
		assert(aggrMIN.size() == aggrCOUNT.size());
		assert(aggrMAX.size() == aggrCOUNT.size());
		assert(aggrSUM.size() == aggrCOUNT.size());

		switch (gAttr.type) {
		case TypeInt:
			if (aggrLookUpInt.count(*(int*) gData) < 1) {
				aggrLookUpInt[*(int*) gData] = aggrCOUNT.size();
				aggrKey.push_back(emptyBuffer);
				aggrMIN.push_back(emptyBuffer);
				aggrMAX.push_back(emptyBuffer);
				aggrSUM.push_back(emptyBuffer);
				memcpy(aggrMIN.at(aggrMIN.size() - 1).data, aggrMINDef,
						sizeof(aggrMINDef));
				memcpy(aggrMAX.at(aggrMAX.size() - 1).data, aggrMAXDef,
						sizeof(aggrMAXDef));
				memcpy(aggrSUM.at(aggrSUM.size() - 1).data, aggrSUMDef,
						sizeof(aggrSUMDef));
				aggrCOUNT.push_back(aggrCOUNTDef);
				*(int*) aggrKey.at(aggrKey.size() - 1).data = *(int*) gData;
			}
			luInd = aggrLookUpInt[*(int*) gData];
			break;
		case TypeReal:
			if (aggrLookUpFloat.count(*(float*) gData) < 1) {
				aggrLookUpFloat[*(float*) gData] = aggrCOUNT.size();
				aggrKey.push_back(emptyBuffer);
				aggrMIN.push_back(emptyBuffer);
				aggrMAX.push_back(emptyBuffer);
				aggrSUM.push_back(emptyBuffer);
				memcpy(aggrMIN.at(aggrMIN.size() - 1).data, aggrMINDef,
						sizeof(aggrMINDef));
				memcpy(aggrMAX.at(aggrMAX.size() - 1).data, aggrMAXDef,
						sizeof(aggrMAXDef));
				memcpy(aggrSUM.at(aggrSUM.size() - 1).data, aggrSUMDef,
						sizeof(aggrSUMDef));
				aggrCOUNT.push_back(aggrCOUNTDef);
				*(float*) aggrKey.at(aggrKey.size() - 1).data = *(float*) gData;
			}
			luInd = aggrLookUpFloat[*(float*) gData];
			break;
		case TypeVarChar:
			gData[*(int*) gData + sizeof(int)] = '\0';

			string gStr(gData + sizeof(int));

			if (aggrLookUpStr.count(gStr) < 1) {
				aggrLookUpStr[gStr] = aggrCOUNT.size();
				aggrKey.push_back(emptyBuffer);
				aggrMIN.push_back(emptyBuffer);
				aggrMAX.push_back(emptyBuffer);
				aggrSUM.push_back(emptyBuffer);
				memcpy(aggrMIN.at(aggrMIN.size() - 1).data, aggrMINDef,
						sizeof(aggrMINDef));
				memcpy(aggrMAX.at(aggrMAX.size() - 1).data, aggrMAXDef,
						sizeof(aggrMAXDef));
				memcpy(aggrSUM.at(aggrSUM.size() - 1).data, aggrSUMDef,
						sizeof(aggrSUMDef));
				aggrCOUNT.push_back(aggrCOUNTDef);
				memcpy(aggrKey.at(aggrKey.size() - 1).data, gData,
						*(int*) gData + sizeof(int));
			}
			luInd = aggrLookUpStr[gStr];
			break;
		}

		switch (op) {
		case MIN:
			if (checkValue(aggAttr.type, LT_OP, attrData,
					aggrMIN.at(luInd).data)) {
				switch (aggAttr.type) {
				case TypeInt:
					*(int*) aggrMIN.at(luInd).data = *(int*) attrData;
					break;
				case TypeReal:
					*(float*) aggrMIN.at(luInd).data = *(float*) attrData;
					break;
				case TypeVarChar:
					memcpy(aggrMIN.at(luInd).data, attrData,
							*(int*) attrData + sizeof(int));
					break;
				}
			}
			break;
		case MAX:
			if (checkValue(aggAttr.type, GT_OP, attrData,
					aggrMAX.at(luInd).data)) {
				switch (aggAttr.type) {
				case TypeInt:
					*(int*) aggrMAX.at(luInd).data = *(int*) attrData;
					break;
				case TypeReal:
					*(float*) aggrMAX.at(luInd).data = *(float*) attrData;
					break;
				case TypeVarChar:
					memcpy(aggrMAX.at(luInd).data, attrData,
							*(int*) attrData + sizeof(int));
					break;
				}
			}
			break;
		case SUM:
		case AVG:
			switch (aggAttr.type) {
			case TypeInt:
				*(int*) aggrSUM.at(luInd).data += *(int*) attrData;
				break;
			case TypeReal:
				*(float*) aggrSUM.at(luInd).data += *(float*) attrData;
				break;
			default:
				return;
			}
			break;
		default:
			break;
		}
		aggrCOUNT.at(luInd) += 1;
	}

	assert(aggrKey.size() == aggrCOUNT.size());
	assert(aggrMIN.size() == aggrCOUNT.size());
	assert(aggrMAX.size() == aggrCOUNT.size());
	assert(aggrSUM.size() == aggrCOUNT.size());
	attrs.push_back(gAttr);
	attrs.push_back(resAttr);

	void* pushData;

	// For each grouped data
	for (unsigned int j = 0; j < aggrCOUNT.size(); ++j) {
		Buffer pushBuffer;
		char* pPush = pushBuffer.data;

		switch (gAttr.type) {
		case TypeInt:
			*(int*) pushBuffer.data = *(int*) aggrKey.at(j).data;
			pPush += sizeof(int);
			break;
		case TypeReal:
			*(float*) pushBuffer.data = *(float*) aggrKey.at(j).data;
			pPush += sizeof(float);
			break;
		case TypeVarChar:
			memcpy(pushBuffer.data, aggrKey.at(j).data,
					*(int*) aggrKey.at(j).data + sizeof(int));
			pPush += *(int*) aggrKey.at(j).data + sizeof(int);
			break;
		}

		// Save the correct data to an allocated memory space
		switch (op) {
		case MIN:
			switch (aggAttr.type) {
			case TypeInt:
				*(int*) pPush = *(int*) aggrMIN.at(j).data;
				break;
			case TypeReal:
				*(float*) pPush = *(float*) aggrMIN.at(j).data;
				break;
			case TypeVarChar:
				memcpy(pPush, aggrMIN.at(j).data,
						*(int*) aggrMIN.at(j).data + sizeof(int));
				break;
			}
			break;
		case MAX:
			switch (aggAttr.type) {
			case TypeInt:
				*(int*) pPush = *(int*) aggrMAX.at(j).data;
				break;
			case TypeReal:
				*(float*) pPush = *(float*) aggrMAX.at(j).data;
				break;
			case TypeVarChar:
				memcpy(pPush, aggrMAX.at(j).data,
						*(int*) aggrMAX.at(j).data + sizeof(int));
				break;
			}
			break;
		case SUM:
			if (aggAttr.type == TypeInt) {
				*(int*) pPush = *(int*) aggrSUM.at(j).data;
			} else if (aggAttr.type == TypeReal) {
				*(float*) pPush = *(float*) aggrSUM.at(j).data;
			}
			break;
		case AVG:
			if (aggAttr.type == TypeInt) { // AVG always returns float
				*(float*) pPush = (float) *(int*) aggrSUM.at(j).data
						/ aggrCOUNT.at(j);
			} else if (aggAttr.type == TypeReal) {
				*(float*) pPush = *(float*) aggrSUM.at(j).data
						/ aggrCOUNT.at(j);
			}
			break;
		case COUNT:
			*(int*) pPush = aggrCOUNT.at(j);
		}

		// And push it into aggData
		unsigned int len = getRecordLength(attrs, pushBuffer.data);

		pushData = malloc(len);
		memcpy(pushData, pushBuffer.data, len);
		this->aggData.push_back((char*) pushData);
	}

	// End of aggregation
	valid = true;
}

void Aggregate::getOpStr(string& str, AggregateOp op) {
	str.erase();
	switch (op) {
	case MIN:
		str.append("MIN");
		break;
	case MAX:
		str.append("MAX");
		break;
	case SUM:
		str.append("SUM");
		break;
	case AVG:
		str.append("AVG");
		break;
	case COUNT:
		str.append("COUNT");
		break;
	}
}

RC Aggregate::getNextTuple(void *data) {
	if (!valid) {
		vector<Attribute>().swap(this->attrs);
		vector<char*>().swap(this->aggData);
		return QE_EOF;
	}
	if (posData >= aggData.size()) {
		return QE_EOF;
	} else {
		unsigned int len = getRecordLength(attrs, aggData.at(posData));

		memcpy(data, aggData.at(posData), len);
		posData += 1;
	}
	return 0;
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const {
	if (valid)
		for (unsigned int j = 0; j < this->attrs.size(); ++j)
			attrs.push_back(this->attrs.at(j));
}
