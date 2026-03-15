<?php
namespace Raxon\Node\Trait;

use Raxon\Module\Filter as Module;

use Exception;

trait Filter {

    /**
     * @throws Exception
     */
    private function filter($record=[], $filter=[], $options=[]): mixed
    {

        $list = [];
        $list[] = $record;
        d($filter);
        d($list);
        $list = Module::list($list)->where($filter);
        breakpoint($list);
        if(!empty($list)){
            return $record;
        }
        return false;
    }
}