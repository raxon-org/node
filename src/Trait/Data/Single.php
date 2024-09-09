<?php
namespace Raxon\Org\Node\Trait\Data;
trait Single {

    public function single($response=[]): false | array
    {
        if(
            $response &&
            array_key_exists('list', $response) &&
            is_array($response['list'])
        ){
            $node = current($response['list']);
            if($node){
                return [
                    'node' => $node
                ];
            }
        }
        elseif(
            $response &&
            array_key_exists('error', $response) &&
            is_array($response['error'])
        ){
            $error = current($response['error']);
            if($error){
                return [
                    'error' => $error
                ];
            }
        }
        return false;
    }
}