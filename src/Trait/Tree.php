<?php

namespace Raxon\Node\Trait;

use Exception;

use Raxon\Exception\FileWriteException;
use Raxon\Exception\ObjectException;

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
        if(!array_key_exists('type', $record)){
            ddd($$record);
        }
        switch($record['type']){
            case 'string':
                return $record['execute'];
            default:
                d($record);
                throw new Exception('Unknown type: ' . $record['type']);
        }
        return null;
    }
}