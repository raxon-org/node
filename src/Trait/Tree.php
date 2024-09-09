<?php

namespace Raxon\Org\Node\Trait;

use Raxon\Org\Module\Parse;
use Raxon\Org\Module\Parse\Token;

use Raxon\Org\Exception\FileWriteException;
use Raxon\Org\Exception\ObjectException;

trait Tree {

    private function tree_max_depth($tree=[]): int
    {
        $depth = 0;
        if(!is_array($tree)){
            return $depth;
        }
        foreach($tree as $nr => $record){
            if(
                is_array($record) &&
                array_key_exists('depth', $record)){
                if($record['depth'] > $depth){
                    $depth = $record['depth'];
                }
            }
        }
        return $depth;
    }

    private function tree_get_set(&$tree, $depth=0): array
    {
        $is_collect = false;
        $set = [];
        foreach($tree as $nr => $record){
            if(
                is_array($record) &&
                array_key_exists('depth', $record) &&
                $record['depth'] === $depth
            ){
                $is_collect = true;
            }
            if($is_collect){
                if(
                    is_array($record) &&
                    array_key_exists('depth', $record) &&
                    $record['depth'] <> $depth){
                    $is_collect = false;
                    break;
                }
                $set[] = $record;
            }
        }
        return $set;
    }

    private function tree_set_replace($tree=[], $set=[], $depth=0): array
    {
        $is_collect = false;
        foreach($tree as $nr => $record){
            if(
                $is_collect === false &&
                is_array($record) &&
                array_key_exists('depth', $record) &&
                $record['depth'] === $depth
            ){
                $is_collect = $nr;
                continue;
            }
            if($is_collect){
                if(
                    is_array($record) &&
                    array_key_exists('depth', $record) &&
                    $record['depth'] <> $depth){
                    $tree[$is_collect] = [];
                    $tree[$is_collect]['set'] = $set;
                    $is_collect = false;
                    break;
                }
                unset($tree[$nr]);
            }
        }
        return $tree;
    }

    /**
     * @throws ObjectException
     * @throws FileWriteException
     */
    private function tree_record_attribute($record=[]): mixed
    {
        $attribute = '';
        if(!array_key_exists('collection', $record)){
            switch($record['type']){
                case Token::TYPE_QUOTE_DOUBLE_STRING:
                    if(strpos($record['value'], '{') === false){
                        return substr($record['value'], 1, -1);
                    }
                    //parse string...
                    $object = $this->object();
                    $storage = $this->storage();
                    $parse = new Parse($object);
                    if(array_key_exists('execute', $record)){
                        return $record['execute'];
                    }
                    if(array_key_exists('parse', $record)){
                        return $parse->compile('{' . $record['parse'] . '}', $storage, $object);
                    } else {
                        $result = $parse->compile($record['value'], $storage, $object);
                    }
                    if(
                        is_string($result) &&
                        substr($result, 0, 1) === '"' &&
                        substr($result, -1) === '"'
                    ){
                        $result = substr($result, 1, -1);
                    }
                    return $result;
                case Token::TYPE_QUOTE_SINGLE_STRING:
                    return substr($record['value'], 1, -1);
            }
            if(array_key_exists('execute', $record)){
                return $record['execute'];
            }
            elseif(array_key_exists('parse', $record)){
                $object = $this->object();
                $storage = $this->storage();
                $parse = new Parse($object);
                if(array_key_exists('parse', $record)){
                    return $parse->compile('{' . $record['parse'] . '}', $storage, $object);
                }
            } else {
                return $record['value'];
            }
        }
        if(!is_array($record['collection'])){
            if(array_key_exists('execute', $record)){
                return $record['execute'];
            }
            elseif(array_key_exists('parse', $record)){
                $object = $this->object();
                $storage = $this->storage();
                $parse = new Parse($object);
                if(array_key_exists('parse', $record)){
                    return $parse->compile('{' . $record['parse'] . '}', $storage, $object);
                }
            } else {
                return substr($record['value'], 1, -1);
            }
        }
        foreach($record['collection'] as $nr => $item){
            if(array_key_exists('execute', $item)){
                $attribute .= $item['execute'];
            }
            elseif(array_key_exists('parse', $item)){
                $object = $this->object();
                $storage = $this->storage();
                $parse = new Parse($object);
                if(array_key_exists('parse', $record)){
                    $attribute .= $parse->compile('{' . $item['parse'] . '}', $storage, $object);
                }
            } else {
                $attribute .= $item['value'];
            }
        }
        return $attribute;
    }

}
