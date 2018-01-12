<?php

namespace Sunspikes\Broadway\Saga;

use Assert\Assertion;
use Broadway\Saga\State;
use Broadway\Saga\State\Criteria;
use Broadway\Saga\State\RepositoryInterface;
use Broadway\Saga\State\StateManagerInterface;
use Broadway\UuidGenerator\UuidGeneratorInterface;

class StateManager implements StateManagerInterface
{
    private $repository;
    private $generator;
    private $creationPolicy;

    /**
     * @param RepositoryInterface    $repository
     * @param UuidGeneratorInterface $generator
     * @param string                 $creationPolicy
     */
    public function __construct(RepositoryInterface $repository, UuidGeneratorInterface $generator, string $creationPolicy)
    {
        $this->assertValidCreationPolicy($creationPolicy);

        $this->repository = $repository;
        $this->generator  = $generator;
        $this->creationPolicy = $creationPolicy;
    }

    /**
     * @param $creationPolicy
     */
    private function assertValidCreationPolicy($creationPolicy)
    {
        $creationPolicyClass = new \ReflectionClass(CreationPolicy::class);
        $validPolicies = $creationPolicyClass->getConstants();

        Assertion::inArray(
            $creationPolicy,
            $validPolicies,
            $creationPolicyClass .' is not a valid CreationPolicy ['. implode(', ', $validPolicies) .']'
        );
    }

    /**
     * {@inheritDoc}
     */
    public function findOneBy($criteria, $sagaId)
    {
        $state = null;
        $creationPolicyCallback = null;

        if ($criteria instanceof Criteria) {
            $comparisions = $criteria->getComparisons();

            if (isset($comparisions['creation_policy_callback'])) {
                $creationPolicyCallback = $comparisions['creation_policy_callback'];
                unset($comparisions['creation_policy_callback']);
                $criteria = new Criteria($comparisions);
            }

            $state = $this->repository->findOneBy($criteria, $sagaId);
        }

        if (null !== $creationPolicyCallback && $creationPolicyCallback instanceof \Closure) {
            if (false === $creationPolicyCallback($state)) {
                return null;
            }
        }

        switch ($this->creationPolicy) {
            case CreationPolicy::IF_NONE_FOUND:
                if ($state) {
                    break;
                }
                // if there is no state, create new
            case CreationPolicy::ALWAYS:
                $state = new State($this->generator->generate());
                break;
            case CreationPolicy::NONE:
                break;
        }

        return $state;
    }
}
