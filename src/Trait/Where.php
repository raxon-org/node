<?php

namespace Raxon\Node\Trait;

use Raxon\Module\Core;
use Raxon\Module\Filter;
use Raxon\Module\Parse\Token;

use Exception;

trait Where {

    private function operator($tree=[]): mixed
    {
        if(!is_array($tree)){
            return $tree;
        }
        $result = [];
        $previous = null;
        $is_collect = false;
        $operator = '';
        foreach($tree as $nr => $record){
            if(array_key_exists($nr - 1, $tree)){
                $previous = $nr - 1;
            }
            if(
                $is_collect === false &&
                array_key_exists('type', $record) &&
                array_key_exists('value', $record) &&
                $record['type'] === Token::TYPE_IS_MINUS &&
                $record['value'] === '-' &&
                array_key_exists($previous, $tree) &&
                array_key_exists('type', $tree[$previous]) &&
                $tree[$previous]['type'] !== Token::TYPE_WHITESPACE
            ) {
                if($previous >= 0){
                    $operator = $tree[$previous]['value'];
                }
                $operator .= $record['value'];
                $is_collect = $nr;
                if($previous >= 0){
                    $tree[$nr]['column'] = $tree[$previous]['column'];
                    $tree[$nr]['row'] = $tree[$previous]['row'];
                    unset($tree[$previous]);
                }
                continue;
            }
            if($is_collect !== false){
                if($record['type'] === Token::TYPE_WHITESPACE){
                    $tree[$is_collect]['value'] = $operator;
                    $tree[$is_collect]['type'] = $operator;
                    $tree[$is_collect]['is_operator'] = true;
                    $is_collect = false;
                    $operator = '';
                    continue;
                }
                $operator .= $record['value'];
                unset($tree[$nr]);
            }
        }
        if($is_collect){
            $tree[$is_collect]['value'] = $operator;
            $tree[$is_collect]['type'] = $operator;
            $tree[$is_collect]['is_operator'] = true;
            $is_collect = false;
        }
        return $tree;
    }

    /**
     * @throws Exception
     */
    private function where_convert($input=[]): array
    {
        if(is_array($input)){
            $is_string = true;
            foreach($input as $nr => $line){
                if(!is_string($line)){
                    $is_string = false;
                    break;
                }
            }
            if($is_string){
                $input = implode(' ', $input);
            }
        }
        $string = $input;
        if(!is_string($string)){
            return $string;
        }
        $options = [
            'with_whitespace' => true,
            'extra_operators' => [
                'and',
                'or',
                'xor'
            ]
        ];
        $tree = Token::tree('{' . $string . '}', $options);
        $tree = $this->operator($tree);
        $is_collect = false;
        $previous = null;
        $next = null;
        foreach($tree as $nr => $record){
            if(array_key_exists($nr - 1, $tree)){
                $previous = $nr - 1;
            }
            if(array_key_exists($nr - 2, $tree)){
                $next = $nr - 2;
            }
            if($record['type'] === Token::TYPE_CURLY_OPEN){
                unset($tree[$nr]);
            }
            elseif($record['type'] === Token::TYPE_CURLY_CLOSE){
                unset($tree[$nr]);
            }
            elseif($record['type'] === Token::TYPE_WHITESPACE){
                if(!empty($collection)){
                    if(array_key_exists($is_collect, $tree)){
                        $tree[$is_collect]['collection'] = $collection;
                        $tree[$is_collect]['type'] = Token::TYPE_COLLECTION;
                        $tree[$is_collect]['value'] = '';
                    }
                    $collection = [];
                }
                $is_collect = false;
                unset($tree[$nr]);
            }
            elseif($record['value'] === '('){
                $tree[$nr] = '(';
            }
            elseif($record['value'] === ')'){
                $tree[$nr] = ')';
            }
            elseif($is_collect === false && $record['value'] === '.'){
                $is_collect = true;
                $collection = [];
                $collection[] = $tree[$previous];
                unset($tree[$previous]);
            }
            elseif(
                in_array(
                    mb_strtolower($record['value']),
                    [
                        'and',
                        'or',
                        'xor'
                    ],
                    true
                )
            ){
                $tree[$nr] = $record['value'];
            }
            if($is_collect === true){
                $collection[] = $record;
                $is_collect = $nr;
            }
            elseif($is_collect){
                if($record['type'] !== Token::TYPE_CURLY_CLOSE){
                    $collection[] = $record;
                }
                unset($tree[$nr]);
            }
        }
        if(!empty($collection)){
            if(array_key_exists($is_collect, $tree)){
                $tree[$is_collect]['collection'] = $collection;
                $tree[$is_collect]['type'] = Token::TYPE_COLLECTION;
                $tree[$is_collect]['value'] = '';
            }
            $collection = [];
        }
        $previous = null;
        $next = null;
        $list = [];
        foreach($tree as $nr => $record){
            $list[] = $record;
            unset($tree[$nr]);
        }
        foreach($list as $nr => $record){
            if(array_key_exists($nr - 1, $list)){
                $previous = $nr - 1;
            }
            if(array_key_exists($nr + 1, $list)){
                $next = $nr + 1;
            }
            if(!is_array($record)){
                continue;
            }
            if(
                array_key_exists('is_operator', $record) &&
                $record['is_operator'] === true
            ){
                $attribute = $this->tree_record_attribute($list[$previous]);
                $operator = $record['value'];
                $value = $this->tree_record_attribute($list[$next]);

                $list[$previous] = [
                    'attribute' => $attribute,
                    'value' => $value,
                    'operator' => $operator
                ];
                unset($list[$nr]);
                unset($list[$next]);
            }
            elseif(
                in_array(
                    mb_strtolower($record['value']),
                    Filter::OPERATOR_LIST_NAME,
                    true
                )
            ){
                $attribute = $this->tree_record_attribute($list[$previous]);
                $operator = mb_strtolower($record['value']);
                $value = $this->tree_record_attribute($list[$next]);
                $list[$previous] = [
                    'attribute' => $attribute,
                    'value' => $value,
                    'operator' => $operator
                ];
                unset($list[$nr]);
                unset($list[$next]);
            }
        }
        $tree = [];
        foreach($list as $nr => $record){
            $tree[] = $record;
            unset($list[$nr]);
        }
        return $tree;
    }

    private function where_get_depth($where=[]): int
    {
        $depth = 0;
        $deepest = 0;
        if(!is_array($where)){
            return $depth;
        }
        foreach($where as $key => $value){
            if($value === '('){
                $depth++;
            }
            if($value === ')'){
                $depth--;
            }
            if($depth > $deepest){
                $deepest = $depth;
            }
        }
        return $deepest;
    }

    private function where_get_set(&$where=[], &$key=null, $deep=0): array
    {
        $set = [];
        $depth = 0;
        //convert where to array.
        if(!is_array($where)){
            return $set;
        }
        foreach($where as $nr => $value){
            if($value === '('){
                $depth++;
            }
            if($value === ')'){
                if($depth === $deep){
                    unset($where[$nr]);
                    if(!empty($set)){
                        break;
                    }
                }
                $depth--;
                if(
                    $depth === $deep &&
                    !empty($set)
                ){
                    break;
                }
            }
            if($depth === $deep){
                if($key === null){
                    $key = $nr;
                }
                if(!in_array($value, [
                    '(',
                    ')'
                ], true)) {
                    $set[] = $value;
                }
                unset($where[$nr]);
            }
        }
        return $set;
    }

    /**
     * @throws Exception
     */
    private function where_process($record, $set=[], &$where=[], &$key=null, &$operator=null, &$index_where=null, $options=[]): ?array
    {
        if(!is_array($set)){
            return [];
        }
//        d($set);
        $count = count($set);
        $set_init = $set;
        if(array_key_exists('debug', $options)){
            d($record);
            d($set);
            d($count);
        }
        if(
            array_key_exists(0, $set) &&
            $count === 1
        ){
            $operator = null;
            if(
                array_key_exists('match', $set[0]) &&
                (
                    $set[0]['match'] === true ||
                    $set[0]['match'] === false
                )
            ){
                $where[$key] = $set[0];
                ksort($where, SORT_NATURAL);
                $where = array_values($where);
                return $set;
            }
            $list = [];
            $list[] = $record;
            if(
                is_array($set[0]) &&
                array_key_exists('attribute', $set[0]) &&
                array_key_exists('value', $set[0]) &&
                array_key_exists('operator', $set[0])
            ){
                $filter_where = [
                    $set[0]['attribute'] => [
                        'value' => $set[0]['value'],
                        'operator' => $set[0]['operator'],
                        'strict' => $set[0]['strict'] ?? true
                    ]
                ];
                $left = Filter::list($list)->where($filter_where);
                if(!empty($left)){
                    $set[0] = [
                        'attribute' => 'uuid',
                        'operator' => '===',
                        'value' => $record->uuid,
                        'match' => true
                    ];
                    $where[$key] = $set[0];
                } else {
                    if(
                        is_array($set[0]) &&
                        array_key_exists('attribute', $set[0]) &&
                        array_key_exists('value', $set[0]) &&
                        array_key_exists('operator', $set[0]) &&
                        property_exists($record, $set[0]['attribute'])
                    ){
                        $index_where[0] = [
                            $set[0]['value'],
                            $record->{$set[0]['attribute']}
                        ];
                    }
                    $set[0]['match'] = false;
                    $where[$key] = [
                        'attribute' => 'uuid',
                        'operator' => '===',
                        'value' => $record->uuid,
                        'match' => false
                    ];
                }
            }
            ksort($where, SORT_NATURAL);
            $where = array_values($where);
            return $set;
        }
        elseif(
            array_key_exists(0, $set) &&
            array_key_exists(1, $set) &&
            array_key_exists(2, $set)
        ){
            switch($set[1]) {
                case 'or':
                    $operator = 'or';
                    if (
                        array_key_exists('match', $set[0]) &&
                        $set[0]['match'] === true
                    ) {
                        $where[$key] = $set[0];
                        unset($where[$key]['match']);
                    }
                    elseif(
                        array_key_exists('match', $set[2]) &&
                        $set[2]['match'] === true
                    ){
                        $where[$key] = $set[2];
                        unset($where[$key]['match']);
                    }
                    $list = [];
                    $list[] = $record;
                    if (
                        array_key_exists('match', $set[0]) &&
                        $set[0]['match'] === false
                    ) {
                        $left = [];
                    }
                    elseif (
                        is_array($set[0]) &&
                        array_key_exists('attribute', $set[0]) &&
                        array_key_exists('value', $set[0]) &&
                        array_key_exists('operator', $set[0])
                    ) {
                        $filter_where = [
                            $set[0]['attribute'] => [
                                'value' => $set[0]['value'],
                                'operator' => $set[0]['operator'],
                                'strict' => $set[0]['strict'] ?? true
                            ]
                        ];
                        $left = Filter::list($list)->where($filter_where);
                    }
                    if(is_array($set[2])){
                        if (
                            array_key_exists('match', $set[2]) &&
                            $set[2]['match'] === false
                        ) {
                            $right = [];
                        } elseif (
                            array_key_exists('attribute', $set[2]) &&
                            array_key_exists('value', $set[2]) &&
                            array_key_exists('operator', $set[2])
                        ) {
                            $filter_where = [
                                $set[2]['attribute'] => [
                                    'value' => $set[2]['value'],
                                    'operator' => $set[2]['operator'],
                                    'strict' => $set[2]['strict'] ?? true
                                ]
                            ];
                            $right = Filter::list($list)->where($filter_where);
                        }
                    }
                    if(array_key_exists('debug', $options)){
                        d($left);
                        d($right);
                    }
                    if (!empty($left)) {
                        $where[$key] = $set[0];
//                        $where[$key]['match'] = true;
                        $set[0] = [
                            'attribute' => 'uuid',
                            'operator' => '===',
                            'value' => $record->uuid,
                            'match' => true
                        ];
                    } else {
                        $set[0]['match'] = false;
                    }
                    if (!empty($right)) {
                        $where[$key] = $set[2];
//                        $where[$key]['match'] = true;
                        $set[2] = [
                            'attribute' => 'uuid',
                            'operator' => '===',
                            'value' => $record->uuid,
                            'match' => true
                        ];
                    } else {
                        $set[2]['match'] = false;
                    }
                    if (!empty($left) || !empty($right)) {
                        //nothing
                    } else {
                        if(
                            is_array($set_init[0]) &&
                            array_key_exists('attribute', $set_init[0]) &&
                            array_key_exists('value', $set_init[0]) &&
                            array_key_exists('operator', $set_init[0]) &&
                            property_exists($record, $set_init[0]['attribute'])
                        ){
                            $index_where[0] = [
                                $set_init[0]['value'],
                                $record->{$set_init[0]['attribute']}
                            ];
                        }
                        if(
                            is_array($set_init[2]) &&
                            array_key_exists('attribute', $set_init[2]) &&
                            array_key_exists('value', $set_init[2]) &&
                            array_key_exists('operator', $set_init[2]) &&
                            property_exists($record, $set_init[2]['attribute'])
                        ){
                            $index_where[2] = [
                                $set_init[2]['value'],
                                $record->{$set_init[2]['attribute']}
                            ];
                        }
                        if(empty($left)){
                            $where[$key] = $set[0];
                            $where[$key]['match'] = false;
                        }
                        if(empty($right)){
                            $where[$key] = $set[2];
                            $where[$key]['match'] = false;
                        }
                    }
                    ksort($where, SORT_NATURAL);
                    $where = array_values($where);

                    if($record->uuid === '3cd80fdf-38fe-40e8-9bfe-4d1d4f281bf1'){
                        d($key);
                        d($where);
                        d($set);
                    }
                    if(array_key_exists('debug', $options)){
                        d($where);
                        d($set);
                    }
                    return $set;
                case 'and':
                    $operator = 'and';
                    $list = [];
                    $list[] = $record;
//                    d($set);
                    if (
                        is_array($set[0]) &&
                        is_array($set[2]) &&
                        array_key_exists('attribute', $set[0]) &&
                        array_key_exists('value', $set[0]) &&
                        array_key_exists('operator', $set[0]) &&
                        array_key_exists('attribute', $set[2]) &&
                        array_key_exists('value', $set[2]) &&
                        array_key_exists('operator', $set[2]) &&
                        !array_key_exists('match', $set[0]) &&
                        !array_key_exists('match', $set[2])
                    ) {
                        $filter_where = [
                            $set[0]['attribute'] => [
                                'value' => $set[0]['value'],
                                'operator' => $set[0]['operator'],
                                'strict' => $set[0]['strict'] ?? true
                            ]
                        ];
                        $left = Filter::list($list)->where($filter_where);
                        $filter_where = [
                            $set[2]['attribute'] => [
                                'value' => $set[2]['value'],
                                'operator' => $set[2]['operator'],
                                'strict' => $set[2]['strict'] ?? true
                            ]
                        ];
                        $right = Filter::list($list)->where($filter_where);
                        if (!empty($left) && !empty($right)) {
                            $set[0] = [
                                'attribute' => 'uuid',
                                'operator' => '===',
                                'value' => $record->uuid,
                                'match' => true
                            ];
                            $set[2] = [
                                'attribute' => 'uuid',
                                'operator' => '===',
                                'value' => $record->uuid,
                                'match' => true
                            ];
                            $where[$key] = $set[0];
                        } else {
                            if(
                                is_array($set[0]) &&
                                array_key_exists('attribute', $set[0]) &&
                                array_key_exists('value', $set[0]) &&
                                array_key_exists('operator', $set[0]) &&
                                property_exists($record, $set[0]['attribute'])
                            ){
                                $index_where[0] = [
                                    $set[0]['value'],
                                    $record->{$set[0]['attribute']}
                                ];
                            }
                            if(
                                is_array($set[2]) &&
                                array_key_exists('attribute', $set[2]) &&
                                array_key_exists('value', $set[2]) &&
                                array_key_exists('operator', $set[2]) &&
                                property_exists($record, $set[2]['attribute'])
                            ){
                                $index_where[2] = [
                                    $set[2]['value'],
                                    $record->{$set[2]['attribute']}
                                ];
                            }
                            if($left){
                                $set[0]['match'] = true;
                            } else {
                                $set[0]['match'] = false;
                                $where[$key] = [
                                    'attribute' => 'uuid',
                                    'operator' => '===',
                                    'value' => $record->uuid,
                                    'match' => false
                                ];
                            }
                            if($right){
                                $set[2]['match'] = true;
                            } else {
                                $set[2]['match'] = false;
                                $where[$key] = [
                                    'attribute' => 'uuid',
                                    'operator' => '===',
                                    'value' => $record->uuid,
                                    'match' => false
                                ];
                            }
                        }
                        ksort($where, SORT_NATURAL);
                        $where = array_values($where);
                        return $set;
                    }
                    /**
                     * more than "1 'and' or 'or'"
                     */
                    elseif(
                        array_key_exists('match', $set[0]) &&
                        $set[0]['match'] === true &&
                        is_array($set[2]) &&
                        array_key_exists('attribute', $set[2]) &&
                        array_key_exists('value', $set[2]) &&
                        array_key_exists('operator', $set[2])
                    ) {
//                        d($set);
                        $filter_where = [
                            $set[2]['attribute'] => [
                                'value' => $set[2]['value'],
                                'operator' => $set[2]['operator'],
                                'strict' => $set[2]['strict'] ?? true
                            ]
                        ];
                        $and = Filter::list($list)->where($filter_where);
                        if(array_key_exists('debug', $options)){
//                            d($set);
//                            d($and);
//                            d($filter_where);
                        }
                        if (!empty($and)) {
                            $set[0] = [
                                'attribute' => 'uuid',
                                'operator' => '===',
                                'value' => $record->uuid,
                                'match' => true
                            ];
                            $set[2] = [
                                'attribute' => 'uuid',
                                'operator' => '===',
                                'value' => $record->uuid,
                                'match' => true
                            ];
                            $where[$key] = $set[0];
                        } else {
                            if(
                                is_array($set[0]) &&
                                array_key_exists('attribute', $set[0]) &&
                                array_key_exists('value', $set[0]) &&
                                array_key_exists('operator', $set[0]) &&
                                property_exists($record, $set[0]['attribute'])
                            ){
                                $index_where[0] = [
                                    $set[0]['value'],
                                    $record->{$set[0]['attribute']}
                                ];
                            }
                            if(
                                is_array($set[2]) &&
                                array_key_exists('attribute', $set[2]) &&
                                array_key_exists('value', $set[2]) &&
                                array_key_exists('operator', $set[2]) &&
                                property_exists($record, $set[2]['attribute'])
                            ){
                                $index_where[2] = [
                                    $set[2]['value'],
                                    $record->{$set[2]['attribute']}
                                ];
                            }
                            $set[2]['match'] = false;
                            $where[$key] = [
                                'attribute' => 'uuid',
                                'operator' => '===',
                                'value' => $record->uuid,
                                'match' => false
                            ];
                        }
                        ksort($where, SORT_NATURAL);
                        $where = array_values($where);
                        return $set;
                    }
                    elseif(
                        array_key_exists('match', $set[0]) &&
                        $set[0]['match'] === false &&
                        array_key_exists(1, $set) &&
                        $set[1] === 'and' &&
                        is_array($set[2]) &&
                        array_key_exists('attribute', $set[2]) &&
                        array_key_exists('value', $set[2]) &&
                        array_key_exists('operator', $set[2])
                    ) {
                        array_shift($set);
                        array_shift($set);
                        array_shift($set);

                        array_unshift($set,
                            [
                                'attribute' => 'uuid',
                                'operator' => '===',
                                'value' => $record->uuid,
                                'match' => false
                            ]
                        );
                        $where[$key] = $set[0];
                    } else {
                        d($where);
                        d($list);
                        ddd($set);
                    }
                    ksort($where, SORT_NATURAL);
                    $where = array_values($where);
                    return $set;
                case 'xor' :
                    $operator = 'xor';
                    $list = [];
                    $list[] = $record;
                    $is_true = 0;
                    $left = null;
                    foreach ($set as $nr => $true) {
                        if(
                            in_array($true, [
                                'and',
                                'or'
                            ], true)
                        ){
                            throw new Exception('And or Or not allowed in Xor, use sets instead.');
                        }
                        elseif (
                            $true === true
                        ) {
                            $is_true++;
                        }
                        elseif (
                            is_array($true) &&
                            array_key_exists('attribute', $true) &&
                            array_key_exists('value', $true) &&
                            array_key_exists('operator', $true)
                        ) {
                            $filter_where = [
                                $true['attribute'] => [
                                    'value' => $true['value'],
                                    'operator' => $true['operator'],
                                    'strict' => $true['strict'] ?? true
                                ]
                            ];
                            $current = Filter::list($list)->where($filter_where);
                            if (!empty($current)) {
                                $is_true++;
                                $set[$nr] = [
                                    'attribute' => 'uuid',
                                    'operator' => '===',
                                    'value' => $record->uuid,
                                    'match' => true
                                ];
                            } else {
                                $set[$nr]['match'] = false;
                            }
                        }
                    }
                    if ($is_true === 1) {
                        $set = [];
                        $set[0] = [
                            'attribute' => 'uuid',
                            'operator' => '===',
                            'value' => $record->uuid,
                            'match' => true

                        ];
                        $where[$key] = $set[0];
                        $operator = null;
                        return $set;
                    }
                    $where[$key]['match'] = false;
                    $set = [];
                    $set[0]['match'] = false;
                    ksort($where, SORT_NATURAL);
                    $where = array_values($where);
                    return $set;
            }
        }
        ksort($where, SORT_NATURAL);
        $where = array_values($where);
        return [];
    }

    /**
     * @throws Exception
     */
    public function where($record, $where=[], $options=[]): false | array | object
    {
        if(empty($where)){
            return $record;
        }
        if(!is_array($where)){
            $where = Core::object($where, Core::OBJECT_ARRAY);
        }
        if(!is_array($options)){
            $options = Core::object($options, Core::OBJECT_ARRAY);
        }
        $deepest = $this->where_get_depth($where);
        $counter =0;
        while($deepest >= 0){
            if($counter > 1024){
                break;
            }
//            d($where);
            $set = $this->where_get_set($where, $key, $deepest);
            if(array_key_exists('debug', $options)){
                d($set);
            }
            if($record->uuid === '3cd80fdf-38fe-40e8-9bfe-4d1d4f281bf1' && $deepest === 0){
                d($set);
            }

//            d($where);
            while($record !== false){
                if(array_key_exists('debug', $options)) {
                    d($set);
                }
                $set = $this->where_process($record, $set, $where, $key, $operator, $index_where, $options);
                if($record->uuid === '3cd80fdf-38fe-40e8-9bfe-4d1d4f281bf1' && $deepest === 0){
                    d($set);
                    d($where);
                    d($operator);
                }
//                d($where);
                if(array_key_exists('debug', $options)){
                    d($where);
//                    d($index_where);
                    d($set);
                }

                if(empty($set) && $deepest === 0){
                    return $record;
                }
                $count_set = count($set);
                if(array_key_exists('debug', $options)){
                    d($record);
                    d($set);
                }

                if($count_set === 1){
                    if($record->uuid === '3cd80fdf-38fe-40e8-9bfe-4d1d4f281bf1'){
                        d($operator);
                        d($deepest);
                        d($set);
                        d($set);
                    }
                    if(
                        array_key_exists('match', $set[0]) &&
                        $operator === null &&
                        $set[0]['match'] === true
                    ){
                        break;
                    } else {
                        if($deepest === 0){
                            $record = false;
                            break 2;
                        } else {
                            break;
                        }
                    }
                }
                elseif($count_set >= 3){
                    switch($operator){
                        case 'and':
                            if(
                                array_key_exists('match', $set[0]) &&
                                array_key_exists('match', $set[2]) &&
                                $set[0]['match'] === true &&
                                $set[2]['match'] === true
                            ){
                                array_shift($set);
                                array_shift($set);
                            } else {
                                array_shift($set);
                                array_shift($set);
                                $set[0]['match'] = false;
                            }
//                            d($set);
                            break;
                        case 'or':
//                            d($set);
                            if(
                                array_key_exists('match', $set[0]) &&
                                $set[0]['match'] === true
                            ){
                                $temp = array_shift($set);
                                array_shift($set);
                                $set[0] = $temp;
                            }
                            elseif(
                                array_key_exists('match', $set[2]) &&
                                $set[2]['match'] === true
                            ){
                                array_shift($set);
                                array_shift($set);
                                $set[0]['match'] = true;
                                if($record->uuid === '3cd80fdf-38fe-40e8-9bfe-4d1d4f281bf1'){
                                    d($key);
                                    d($where);
                                    d($set);
                                }
                            }
                            else {
                                array_shift($set);
                                array_shift($set);
                                $set[0]['match'] = false;
                            }
                            if(array_key_exists('debug', $options)){
                                d($set);
                            }
                            break;
                        default:
                            throw new Exception('Unknown operator: ' . $operator);
                    }
                }
                $counter++;
                if($counter > 1024){
                    d('counter break');
                    break 2;
                }
            }
            if($record === false){
                break;
            }
            if($deepest === 0){
                break;
            }
//            ksort($where, SORT_NATURAL);
            $deepest = $this->where_get_depth($where);
            unset($key);
            $counter++;
            if($record->uuid === '3cd80fdf-38fe-40e8-9bfe-4d1d4f281bf1'){
                d($deepest);
                d($where);
                d($set);
            }
        }
        if(array_key_exists('debug', $options)){
//            d($record);
//            d('here2');
        }
        return $record;
    }
}